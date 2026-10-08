#!/usr/bin/env bash
# Seed the bench7 database (~31-33 GB with the defaults) in the native Postgres.
#
# All ids are UUIDv7 generated in Postgres with uuidv7(shift) so they are
# time-ordered and match created_at (= uuid_extract_timestamp(id)). Rows are
# generated with generate_series in chunks (each chunk commits on its own, the
# progress is visible). Counters are exact: users.posts_count = number of
# posts, posts.like_count = number of likes, conversations.message_count =
# number of messages. No column defaults: every INSERT supplies every column.
#
# Usage: ./db/seed.sh            (env overrides below)
# Size model (bodies are 2900 chars = 3 KB payload, STORAGE EXTERNAL):
#   posts 7M ~ 25 GB, messages 1.4M ~ 4.8 GB, likes 8M ~ 1.9 GB, users 500k ~ 0.2 GB
set -euo pipefail
SEED_USERS=${SEED_USERS:-500000}
SEED_POSTS=${SEED_POSTS:-7000000}
SEED_MESSAGES=${SEED_MESSAGES:-1400000}
SEED_LIKES=${SEED_LIKES:-8000000}
MSGS_PER_CONV=${MSGS_PER_CONV:-5}
BODY_CHARS=${BODY_CHARS:-2900}
CHUNK=${CHUNK:-250000}
PUBLIC_PCT=${PUBLIC_PCT:-85}
HERE=$(cd "$(dirname "$0")" && pwd)
ENV_FILE=${ENV_FILE:-$HERE/../bench7.env}
EXPORT=${EXPORT:-$HERE/../results/seed-export.json}
PGPASSWORD=$(grep '^PG_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)
export PGPASSWORD

psql() { command psql -v ON_ERROR_STOP=1 -X -q -h 127.0.0.1 -U bench -d bench "$@"; }
ts() { date +%H:%M:%S; }
h() { echo "(hashint8($1) & 2147483647)"; }   # deterministic 31-bit hash

psql < "$HERE/schema.sql"
if [ "$(psql -tAc "SELECT count(*) FROM bench_meta WHERE k = 'seeded_at'")" != 0 ]; then
  echo "already seeded (bench_meta.seeded_at set); drop the tables to re-seed" >&2; exit 1
fi
for t in users posts conversations messages likes; do
  psql -c "ALTER TABLE $t SET (autovacuum_enabled = off)"
done

# 5114-char base text; a body is a BODY_CHARS window of it at a per-row offset
psql -c "CREATE UNLOGGED TABLE IF NOT EXISTS seed_text AS SELECT string_agg(md5(i::text), ' ') AS t FROM generate_series(1, 155) i"

# ---- id maps (unlogged, dropped at the end): seed number n -> uuid ----
echo "$(ts) id maps"
# users joined over the last ~405 days, one every 70 s
psql -c "CREATE UNLOGGED TABLE seed_users AS
           SELECT g::int AS n, uuidv7(make_interval(secs => -(($SEED_USERS - g) * 70.0))) AS id
           FROM generate_series(1, $SEED_USERS) g" \
     -c "ALTER TABLE seed_users ADD PRIMARY KEY (n)"
# posts over the last ~162 days, one every 2 s; author + visibility fixed here
psql -c "CREATE UNLOGGED TABLE seed_posts AS
           SELECT g AS n, uuidv7(make_interval(secs => -(($SEED_POSTS - g) * 2.0))) AS id,
                  (1 + $(h g) % $SEED_USERS)::int AS u,
                  (CASE WHEN $(h 'g * 7') % 100 < $PUBLIC_PCT THEN 0 ELSE 1 END)::int2 AS vis
           FROM generate_series(1, $SEED_POSTS::int8) g" \
     -c "ALTER TABLE seed_posts ADD PRIMARY KEY (n)"
# conversation pairs (deduplicated, user a < user b); ~MSGS_PER_CONV messages each
CONVS=$(( SEED_MESSAGES / MSGS_PER_CONV ))
psql -c "CREATE UNLOGGED TABLE seed_conv AS
           SELECT row_number() OVER (ORDER BY min(p))::int AS p, a, b,
                  uuidv7(make_interval(secs => -(($SEED_MESSAGES * 5.0) + 3600 + min(p)))) AS id
           FROM (SELECT p, least(x, y) AS a, greatest(x, y) AS b
                 FROM (SELECT p, (1 + $(h 'p * 13') % $SEED_USERS)::int AS x, (1 + $(h 'p * 17') % $SEED_USERS)::int AS y0
                       FROM generate_series(1, $CONVS) p) s,
                      LATERAL (SELECT CASE WHEN y0 = x THEN x % $SEED_USERS + 1 ELSE y0 END AS y) l) d
           GROUP BY a, b" \
     -c "ALTER TABLE seed_conv ADD PRIMARY KEY (p)"
CONVS=$(psql -tAc "SELECT count(*) FROM seed_conv")

run_chunks() { # table total template
  local table=$1 total=$2 tmpl=$3 lo=1 hi
  while [ "$lo" -le "$total" ]; do
    hi=$(( lo + CHUNK - 1 )); [ "$hi" -gt "$total" ] && hi=$total
    psql -c "SET synchronous_commit = off" -c "$(sed "s/:lo/$lo/g; s/:hi/$hi/g" <<<"$tmpl")" >/dev/null
    echo "$(ts) $table $hi / $total"
    lo=$(( hi + 1 ))
  done
}

# ---- likes first (so posts.like_count can be exact), one every 1 s ----
LIKES_SQL="INSERT INTO likes (id, user_id, post_id, created_at)
  SELECT l.id, su.id, sp.id, uuid_extract_timestamp(l.id)
  FROM (SELECT g, uuidv7(make_interval(secs => -(($SEED_LIKES - g) * 1.0))) AS id
        FROM generate_series(:lo::int8, :hi::int8) g) l
  JOIN seed_users su ON su.n = 1 + $(h 'l.g') % $SEED_USERS
  JOIN seed_posts sp ON sp.n = 1 + $(h 'l.g * 31') % $SEED_POSTS
  ON CONFLICT DO NOTHING"
run_chunks likes "$SEED_LIKES" "$LIKES_SQL"

echo "$(ts) counters"
psql -c "CREATE UNLOGGED TABLE seed_like_counts AS SELECT post_id, count(*) AS n FROM likes GROUP BY post_id" \
     -c "ALTER TABLE seed_like_counts ADD PRIMARY KEY (post_id)" \
     -c "CREATE UNLOGGED TABLE seed_user_posts AS SELECT u, count(*) AS n FROM seed_posts GROUP BY u" \
     -c "ALTER TABLE seed_user_posts ADD PRIMARY KEY (u)"

echo "$(ts) users $SEED_USERS"
psql -c "INSERT INTO users (id, username, email, display_name, bio, avatar_url, location, website,
                            is_verified, is_private, status, followers_count, following_count, posts_count,
                            created_at, updated_at, last_active_at)
  SELECT su.id, 'user' || su.n, 'user' || su.n || '@bench7.dev', 'User ' || su.n,
         substr(s.t, 1 + (su.n % 500), 120),
         CASE WHEN $(h 'su.n * 3') % 10 < 8 THEN 'https://cdn.bench7.dev/a/' || su.n || '.jpg' END,
         (ARRAY['Singapore','Mumbai','Berlin','Austin','Tokyo','Sao Paulo','Lagos','London',NULL])[1 + $(h 'su.n * 5') % 9],
         CASE WHEN $(h 'su.n * 11') % 10 < 3 THEN 'https://user' || su.n || '.example.com' END,
         $(h 'su.n * 19') % 100 = 0, $(h 'su.n * 23') % 20 = 0, 0::int2,
         $(h 'su.n * 29') % 5000, $(h 'su.n * 37') % 1000, coalesce(up.n, 0),
         uuid_extract_timestamp(su.id), uuid_extract_timestamp(su.id),
         now() - make_interval(secs => $(h 'su.n * 41') % 2592000)
  FROM seed_users su CROSS JOIN seed_text s LEFT JOIN seed_user_posts up ON up.u = su.n"

POSTS_SQL="INSERT INTO posts (id, user_id, reply_to_id, visibility, title, body, lang, media_url,
                              like_count, comment_count, share_count, view_count, is_edited,
                              created_at, updated_at, deleted_at)
  SELECT sp.id, su.id, NULL, sp.vis,
         'Post ' || sp.n || ' about ' || substr(s.t, 1 + (sp.n % 400)::int, 40),
         substr(s.t, 1 + (sp.n % 1000)::int, $BODY_CHARS),
         (ARRAY['en','en','en','en','es','fr','de','hi','ja','pt'])[1 + $(h 'sp.n * 3') % 10],
         CASE WHEN $(h 'sp.n * 5') % 5 = 0 THEN 'https://cdn.bench7.dev/m/' || sp.n || '.jpg' END,
         coalesce(lc.n, 0), 0, $(h 'sp.n * 11') % 50, coalesce(lc.n, 0) * 10 + $(h 'sp.n * 13') % 1000,
         $(h 'sp.n * 17') % 20 = 0,
         uuid_extract_timestamp(sp.id), uuid_extract_timestamp(sp.id),
         CASE WHEN $(h 'sp.n * 19') % 200 = 0 THEN uuid_extract_timestamp(sp.id) + interval '1 day' END
  FROM seed_posts sp
  JOIN seed_users su ON su.n = sp.u
  LEFT JOIN seed_like_counts lc ON lc.post_id = sp.id
  CROSS JOIN seed_text s
  WHERE sp.n BETWEEN :lo AND :hi"
run_chunks posts "$SEED_POSTS" "$POSTS_SQL"

# messages over the last ~81 days, one every 5 s, each in a seeded conversation
MSG_SQL="INSERT INTO messages (id, conversation_id, sender_id, recipient_id, content_type, body, attachment_url,
                               created_at, read_at, edited_at, deleted_at)
  SELECT m.id, c.id,
         CASE WHEN $(h 'm.g * 7') % 2 = 0 THEN ua.id ELSE ub.id END,
         CASE WHEN $(h 'm.g * 7') % 2 = 0 THEN ub.id ELSE ua.id END,
         m.ct, substr(s.t, 1 + (m.g % 1000)::int, $BODY_CHARS),
         CASE WHEN m.ct > 0 THEN 'https://cdn.bench7.dev/f/' || m.g END,
         uuid_extract_timestamp(m.id),
         CASE WHEN $(h 'm.g * 11') % 10 < 7 THEN uuid_extract_timestamp(m.id) + interval '30 seconds' END,
         NULL, NULL
  FROM (SELECT g, uuidv7(make_interval(secs => -(($SEED_MESSAGES - g) * 5.0))) AS id,
               (CASE $(h 'g * 3') % 20 WHEN 16 THEN 1 WHEN 17 THEN 1 WHEN 18 THEN 2 WHEN 19 THEN 3 ELSE 0 END)::int2 AS ct
        FROM generate_series(:lo::int8, :hi::int8) g) m
  JOIN seed_conv c ON c.p = 1 + $(h 'm.g') % $CONVS
  JOIN seed_users ua ON ua.n = c.a
  JOIN seed_users ub ON ub.n = c.b
  CROSS JOIN seed_text s"
run_chunks messages "$SEED_MESSAGES" "$MSG_SQL"

echo "$(ts) conversations"
psql -c "SET work_mem = '256MB'" -c "INSERT INTO conversations (id, user_a_id, user_b_id, last_message_id, last_message_preview,
                                    last_message_at, message_count, created_at, updated_at)
  SELECT c.id, least(ua.id, ub.id), greatest(ua.id, ub.id), m.id, left(m.body, 100), m.created_at, agg.n,
         uuid_extract_timestamp(c.id), m.created_at
  FROM (SELECT DISTINCT ON (conversation_id) conversation_id, id AS last_id,
               count(*) OVER (PARTITION BY conversation_id) AS n
        FROM messages ORDER BY conversation_id, id DESC) agg
  JOIN seed_conv c ON c.id = agg.conversation_id
  JOIN seed_users ua ON ua.n = c.a
  JOIN seed_users ub ON ub.n = c.b
  JOIN messages m ON m.id = agg.last_id"

echo "$(ts) export for the load generator"
mkdir -p "$(dirname "$EXPORT")"
# users: every 10th (50k) ; public cursors: 20k public non-deleted post ids spread
# over the whole table ; like targets: 20k post ids
# Kept in bench_meta too: the ids are time-based (uuidv7), so they differ on every seed and the app
# host (run_<fw>.sh) reads them from the database it actually talks to.
psql -c "INSERT INTO bench_meta (k, v) SELECT 'seed_export', json_build_object(
  'users', (SELECT json_agg(id ORDER BY n) FROM seed_users WHERE n % 10 = 0),
  'public_cursors', (SELECT json_agg(p.id ORDER BY sp.n) FROM seed_posts sp JOIN posts p ON p.id = sp.id
                      WHERE sp.vis = 0 AND p.deleted_at IS NULL AND sp.n % (greatest($SEED_POSTS / 20000, 1)) = 0),
  'like_posts', (SELECT json_agg(id ORDER BY n) FROM seed_posts WHERE n % (greatest($SEED_POSTS / 20000, 1)) = 7 % (greatest($SEED_POSTS / 20000, 1))),
  'readers', (SELECT json_agg(u ORDER BY u) FROM (
      SELECT recipient_id u FROM messages WHERE deleted_at IS NULL GROUP BY 1 HAVING count(*) >= 5
      INTERSECT
      SELECT user_id FROM posts WHERE visibility = 1 AND deleted_at IS NULL GROUP BY 1 HAVING count(*) >= 5) r),
  'body_chars', $BODY_CHARS)::text
  ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
psql -tAc "SELECT v FROM bench_meta WHERE k = 'seed_export'" > "$EXPORT"
echo "$(ts) wrote $EXPORT ($(du -h "$EXPORT" | cut -f1))"

echo "$(ts) indexes + foreign keys"
psql < "$HERE/constraints.sql"

echo "$(ts) vacuum analyze"
psql -c "DROP TABLE seed_text, seed_users, seed_posts, seed_conv, seed_like_counts, seed_user_posts"
# autovacuum stays OFF on the bench tables: the run's updates cross the 10%/20%
# thresholds mid-ramp and an autovacuum then saturates the disk (seen in axum-v2).
# db/reset.sql vacuums + analyzes them between runs instead.
for t in users posts conversations messages likes; do
  psql -c "VACUUM (ANALYZE, FREEZE) $t"
done
psql -c "INSERT INTO bench_meta (k, v) VALUES
           ('users_max_id', (SELECT id FROM users ORDER BY id DESC LIMIT 1)::text),
           ('posts_max_id', (SELECT id FROM posts ORDER BY id DESC LIMIT 1)::text),
           ('conversations_max_id', (SELECT id FROM conversations ORDER BY id DESC LIMIT 1)::text),
           ('messages_max_id', (SELECT id FROM messages ORDER BY id DESC LIMIT 1)::text),
           ('likes_max_id', (SELECT id FROM likes ORDER BY id DESC LIMIT 1)::text),
           ('users', (SELECT count(*) FROM users)::text),
           ('posts', (SELECT count(*) FROM posts)::text),
           ('conversations', (SELECT count(*) FROM conversations)::text),
           ('messages', (SELECT count(*) FROM messages)::text),
           ('likes', (SELECT count(*) FROM likes)::text),
           ('body_chars', '$BODY_CHARS'),
           ('seed_db_bytes', pg_database_size('bench')::text),
           ('seeded_at', now()::text)
         ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
psql -c "CHECKPOINT"
echo "$(ts) done"
psql -c "SELECT k, v FROM bench_meta WHERE k <> 'seed_export' ORDER BY k" \
     -c "SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) FROM pg_class
         WHERE relname IN ('users','posts','conversations','messages','likes') ORDER BY 1" \
     -c "SELECT pg_size_pretty(pg_database_size('bench')) AS db_size"

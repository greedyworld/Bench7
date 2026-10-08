-- Undo everything a benchmark run wrote, so every framework starts from the
-- same data set (and the 40 GB disk does not fill). Run-time ids are UUIDv7
-- generated after the seed, so "id > seeded max id" selects exactly the run's
-- rows. Counters the run incremented are recomputed for the touched rows only.
-- Run with: psql -h 127.0.0.1 -U bench -d bench -f db/reset.sql
\timing on
\set ON_ERROR_STOP on
BEGIN;
CREATE TEMP TABLE wm ON COMMIT DROP AS
  SELECT (SELECT v::uuid FROM bench_meta WHERE k = 'posts_max_id')         AS posts,
         (SELECT v::uuid FROM bench_meta WHERE k = 'messages_max_id')      AS messages,
         (SELECT v::uuid FROM bench_meta WHERE k = 'likes_max_id')         AS likes,
         (SELECT v::uuid FROM bench_meta WHERE k = 'conversations_max_id') AS convs,
         (SELECT v::timestamptz FROM bench_meta WHERE k = 'seeded_at')     AS seeded_at;

CREATE TEMP TABLE touched_posts ON COMMIT DROP AS
  SELECT DISTINCT post_id AS id FROM likes, wm WHERE likes.id > wm.likes
  UNION
  SELECT DISTINCT reply_to_id FROM posts, wm WHERE posts.id > wm.posts AND reply_to_id IS NOT NULL;
CREATE TEMP TABLE touched_users ON COMMIT DROP AS
  SELECT DISTINCT user_id AS id FROM posts, wm WHERE posts.id > wm.posts;
CREATE TEMP TABLE touched_convs ON COMMIT DROP AS
  SELECT DISTINCT conversation_id AS id FROM messages, wm WHERE messages.id > wm.messages;

DELETE FROM likes    USING wm WHERE likes.id > wm.likes;
DELETE FROM messages USING wm WHERE messages.id > wm.messages;
DELETE FROM conversations USING wm WHERE conversations.id > wm.convs;
DELETE FROM posts    USING wm WHERE posts.id > wm.posts;

UPDATE posts p SET like_count    = (SELECT count(*) FROM likes l WHERE l.post_id = p.id),
                   comment_count = (SELECT count(*) FROM posts c WHERE c.reply_to_id = p.id)
  FROM touched_posts t WHERE p.id = t.id;
UPDATE users u SET posts_count = (SELECT count(*) FROM posts p WHERE p.user_id = u.id)
  FROM touched_users t WHERE u.id = t.id;
UPDATE conversations c SET last_message_id = m.id, last_message_preview = left(m.body, 100),
                           last_message_at = m.created_at, updated_at = m.created_at,
                           message_count = (SELECT count(*) FROM messages x WHERE x.conversation_id = c.id)
  FROM touched_convs t
  CROSS JOIN LATERAL (SELECT id, body, created_at FROM messages WHERE conversation_id = t.id ORDER BY id DESC LIMIT 1) m
  WHERE c.id = t.id;
SELECT (SELECT count(*) FROM touched_posts) AS posts_fixed, (SELECT count(*) FROM touched_users) AS users_fixed,
       (SELECT count(*) FROM touched_convs) AS convs_fixed;
COMMIT;

-- maintenance happens here, between runs, never inside a measurement:
-- autovacuum is off on the bench tables (idempotent, also covers older seeds)
ALTER TABLE likes SET (autovacuum_enabled = off);
ALTER TABLE posts SET (autovacuum_enabled = off);
ALTER TABLE users SET (autovacuum_enabled = off);
ALTER TABLE conversations SET (autovacuum_enabled = off);
ALTER TABLE messages SET (autovacuum_enabled = off);
VACUUM (ANALYZE) likes;
VACUUM (ANALYZE) posts;
VACUUM (ANALYZE) users;
VACUUM (ANALYZE) conversations;
VACUUM (ANALYZE) messages;
CHECKPOINT;
SELECT pg_size_pretty(pg_database_size('bench')) AS db_size;

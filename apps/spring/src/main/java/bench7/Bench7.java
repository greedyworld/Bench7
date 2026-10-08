// bench7 — Spring Boot 4 (Spring MVC on virtual threads + HikariCP/pgjdbc + Caffeine + java-jwt).
// Same endpoints, SQL, cache, batcher and JWT rules as the other apps.
//   - ids: UUIDv7 generated here; created_at = uuid_extract_timestamp(id)
//   - cache TTL: deterministic hash bucketing (spreads expiries, no thundering herd)
//   - writes: per-table batcher, lanes keyed by user / conversation / post -> column arrays ->
//     one unnest upsert statement per batch; the reply is sent after commit
package bench7;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class Bench7 {
    public static void main(String[] args) {
        SpringApplication.run(Bench7.class, args);
    }
}

@RestController
class Api {
    static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? d : v;
    }

    static int envInt(String k, int d) {
        try { return Integer.parseInt(System.getenv(k)); } catch (Exception e) { return d; }
    }

    final HikariDataSource db;
    final Cache<String, byte[]> cache;
    final ConcurrentHashMap<String, CompletableFuture<byte[]>> flights = new ConcurrentHashMap<>();
    final HashTtl ttl;
    final Batcher<NewPost> posts;
    final Batcher<NewMessage> messages;
    final Batcher<NewLike> likes;
    final Algorithm alg;
    final JWTVerifier verifier;
    final byte[] issuerKey;
    final String iss, aud;
    final int jwtTtl;
    final Semaphore dbGate; // null when DB_CONCURRENCY <= 0
    final String nPosts = Sql.posts(Sql.POSTS_ONE);
    final String nMessages = Sql.messages(Sql.MESSAGES_ONE);
    final String nLikes = Sql.likes(Sql.LIKES_ONE);

    Api(Environment spring) {
        int poolSize = envInt("DB_POOL_TOTAL", 12);
        int dbConcurrency = envInt("DB_CONCURRENCY", 0);
        dbGate = dbConcurrency > 0 ? new Semaphore(dbConcurrency, true) : null;
        String secret = env("JWT_SECRET", "");
        issuerKey = env("TOKEN_ISSUER_KEY", "").getBytes(StandardCharsets.UTF_8);
        if (secret.isEmpty() || issuerKey.length == 0) throw new IllegalStateException("JWT_SECRET and TOKEN_ISSUER_KEY are required");
        iss = env("JWT_ISS", "bench7");
        aud = env("JWT_AUD", "bench7-api");
        jwtTtl = envInt("JWT_TTL_S", 3600);

        // ---------- database ----------
        URI u = URI.create(env("DATABASE_URL", ""));
        String[] ui = u.getRawUserInfo().split(":", 2);
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl("jdbc:postgresql://" + u.getHost() + ":" + (u.getPort() > 0 ? u.getPort() : 5432) + u.getPath());
        hc.setUsername(URLDecoder.decode(ui[0], StandardCharsets.UTF_8));
        if (ui.length > 1) hc.setPassword(URLDecoder.decode(ui[1], StandardCharsets.UTF_8));
        hc.setMaximumPoolSize(poolSize);
        hc.setMinimumIdle(poolSize);
        hc.setAutoCommit(true);
        hc.setPoolName("bench7");
        int stmtTimeout = envInt("STATEMENT_TIMEOUT_MS", 5000);
        if (stmtTimeout > 0) hc.addDataSourceProperty("options", "-c statement_timeout=" + stmtTimeout);
        db = new HikariDataSource(hc);

        // ---------- cache ----------
        ttl = new HashTtl(envInt("CACHE_TTL_MIN_MS", 45000), envInt("CACHE_TTL_MAX_MS", 60000), Math.max(1, envInt("CACHE_TTL_BINS", 15)));
        cache = Caffeine.newBuilder()
            .maximumWeight((long) envInt("CACHE_MAX_MB", 64) << 20)
            .weigher((String k, byte[] v) -> v.length)
            .expireAfter(new Expiry<String, byte[]>() {
                public long expireAfterCreate(String k, byte[] v, long now) { return TimeUnit.MILLISECONDS.toNanos(ttl.ms(k)); }
                public long expireAfterUpdate(String k, byte[] v, long now, long cur) { return expireAfterCreate(k, v, now); }
                public long expireAfterRead(String k, byte[] v, long now, long cur) { return cur; }
            })
            .build();

        // ---------- batchers ----------
        int max = Math.max(1, envInt("BATCH_MAX_ROWS", 1000));
        int windowMs = envInt("BATCH_WINDOW_MS", 200);
        long window = TimeUnit.MILLISECONDS.toNanos(windowMs);
        int lanes = Math.max(1, envInt("BATCH_LANES", 4));
        int queueMax = envInt("WRITE_QUEUE_MAX", 40000);
        posts = new Batcher<>(db, new PostsTable(), max, window, lanes, queueMax);
        messages = new Batcher<>(db, new MessagesTable(), max, window, lanes, queueMax);
        likes = new Batcher<>(db, new LikesTable(), max, window, lanes, queueMax);
        if (env("BATCH_STATS", "0").equals("1")) {
            List<Batcher<?>> all = List.of(posts, messages, likes);
            Thread.ofPlatform().daemon().name("batch-stats").start(() -> {
                while (true) {
                    try { Thread.sleep(10_000); } catch (InterruptedException e) { return; }
                    for (Batcher<?> b : all) b.report();
                }
            });
        }
        System.err.println("spring batch_rows=" + max + " batch_window_ms=" + windowMs + " lanes=" + lanes
            + " queue_max=" + queueMax + " stmt_timeout_ms=" + stmtTimeout);

        // ---------- JWT ----------
        alg = Algorithm.HMAC256(secret);
        verifier = JWT.require(alg)
            .withIssuer(iss)
            .withAudience(aud)
            .withClaimPresence("exp")
            .withClaimPresence("iat")
            .acceptLeeway(0)
            .build();

        System.out.println("spring listening on " + env("HOST", "0.0.0.0") + ":" + env("PORT", "8080")
            + " pool=" + poolSize);
        List<String> gcs = new ArrayList<>();
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) gcs.add(gc.getName());
        System.out.println("runtime java=" + Runtime.version() + " cpus=" + Runtime.getRuntime().availableProcessors()
            + " gc=" + gcs + " max_heap_mb=" + (Runtime.getRuntime().maxMemory() >> 20)
            + " virtual_threads=" + spring.getProperty("spring.threads.virtual.enabled", Boolean.class, false)
            + " db_concurrency=" + dbConcurrency);
    }

    // optional DB_CONCURRENCY limiter for request threads: acquire before borrowing a connection, release in finally
    void gateAcquire() throws SQLException {
        if (dbGate == null) return;
        try {
            dbGate.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted waiting for db permit", e);
        }
    }

    void gateRelease() {
        if (dbGate != null) dbGate.release();
    }

    interface Binder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    // group 2: one write statement on the pool in the request thread; retried on deadlock / serialization failure
    int execOne(String name, String sql, Binder b) {
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                gateAcquire();
                try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
                    b.bind(ps);
                    ps.executeUpdate();
                    return 200;
                } finally {
                    gateRelease();
                }
            } catch (SQLException e) {
                String st = e.getSQLState();
                if (attempt < 2 && ("40P01".equals(st) || "40001".equals(st))) continue;
                System.err.println("write " + name + " failed: " + e.getMessage());
                return 500;
            } catch (Exception e) {
                System.err.println("write " + name + " failed: " + e);
                return 500;
            }
        }
        return 500;
    }

    static void setUuid(PreparedStatement ps, int i, UUID u) throws SQLException {
        if (u == null) ps.setNull(i, Types.OTHER); else ps.setObject(i, u);
    }

    UUID auth(HttpServletRequest req) {
        String h = req.getHeader("Authorization");
        if (h == null || !h.startsWith("Bearer ")) return null;
        try {
            // verifier checks HS256 only, signature, exp (required), iat (required, not in future), iss, aud; leeway 0
            return H.uuid(verifier.verify(h.substring(7)).getSubject());
        } catch (JWTVerificationException e) {
            return null;
        }
    }

    byte[] queryPage(String sql, boolean isPost, UUID a, UUID b) throws SQLException, IOException {
        gateAcquire();
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, a);
            if (b != null) ps.setObject(2, b);
            ByteArrayOutputStream buf = new ByteArrayOutputStream(8192);
            try (ResultSet r = ps.executeQuery(); JsonGenerator w = H.JSON.createGenerator(buf)) {
                w.writeStartObject();
                w.writeArrayFieldStart("items");
                int n = 0;
                String last = null;
                while (r.next()) {
                    last = isPost ? H.writePost(w, r) : H.writeMessage(w, r);
                    n++;
                }
                w.writeEndArray();
                if (n == 5) w.writeStringField("next", last); else w.writeNullField("next");
                w.writeEndObject();
            }
            return buf.toByteArray();
        } finally {
            gateRelease();
        }
    }

    // ---------- HTTP ----------
    @GetMapping({"/health", "/n/health"})
    void health(HttpServletResponse res) throws IOException {
        H.send(res, 200, H.HEALTH);
    }

    @PostMapping("/auth/token")
    void token(HttpServletRequest req, HttpServletResponse res) throws IOException {
        byte[] hk = String.valueOf(req.getHeader("x-issuer-key")).getBytes(StandardCharsets.UTF_8);
        if (req.getHeader("x-issuer-key") == null || !MessageDigest.isEqual(hk, issuerKey)) { H.fail(res, 401); return; }
        JsonNode o = H.readJson(req, res);
        if (o == null) return;
        JsonNode s = H.field(o, "user_id");
        UUID uid = s != null && s.isTextual() ? H.uuid(s.textValue()) : null;
        if (uid == null) { H.fail(res, 400); return; }
        boolean exists;
        try {
            gateAcquire();
            try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(Sql.USER_EXISTS)) {
                ps.setObject(1, uid);
                try (ResultSet r = ps.executeQuery()) { r.next(); exists = r.getBoolean(1); }
            } finally {
                gateRelease();
            }
        } catch (SQLException e) {
            H.fail(res, 500);
            return;
        }
        if (!exists) { H.fail(res, 404); return; }
        long now = System.currentTimeMillis() / 1000;
        String tok = JWT.create()
            .withSubject(uid.toString())
            .withIssuer(iss)
            .withAudience(aud)
            .withIssuedAt(Instant.ofEpochSecond(now))
            .withExpiresAt(Instant.ofEpochSecond(now + jwtTtl))
            .sign(alg);
        H.send(res, 200, ("{\"token\":\"" + tok + "\",\"expires_in\":" + jwtTtl + "}").getBytes(StandardCharsets.UTF_8));
    }

    @GetMapping("/posts/public")
    void publicPosts(HttpServletRequest req, HttpServletResponse res) throws IOException {
        UUID before = H.cursor(req);
        if (before == null) { H.fail(res, 400); return; }
        String k = before.toString();
        byte[] hit = cache.getIfPresent(k);
        if (hit != null) {
            res.setHeader("x-cache", "hit");
            H.send(res, 200, hit);
            return;
        }
        CompletableFuture<byte[]> mine = new CompletableFuture<>();
        CompletableFuture<byte[]> lead = flights.putIfAbsent(k, mine);
        byte[] b;
        if (lead == null) {
            try {
                b = queryPage(Sql.PUBLIC, true, before, null);
                cache.put(k, b);
                mine.complete(b);
            } catch (Exception e) {
                mine.completeExceptionally(e);
                H.fail(res, 500);
                return;
            } finally {
                flights.remove(k, mine);
            }
        } else {
            try { b = lead.join(); } catch (Exception e) { H.fail(res, 500); return; }
        }
        res.setHeader("x-cache", "miss");
        H.send(res, 200, b);
    }

    // group 2: same SQL every request, no cache, no x-cache header
    @GetMapping("/n/posts/public")
    void nPublicPosts(HttpServletRequest req, HttpServletResponse res) throws IOException {
        UUID before = H.cursor(req);
        if (before == null) { H.fail(res, 400); return; }
        byte[] b;
        try { b = queryPage(Sql.PUBLIC, true, before, null); } catch (Exception e) { H.fail(res, 500); return; }
        H.send(res, 200, b);
    }

    @GetMapping({"/posts/private", "/n/posts/private"})
    void privatePosts(HttpServletRequest req, HttpServletResponse res) throws IOException {
        page(req, res, Sql.PRIVATE, true);
    }

    @GetMapping({"/messages", "/n/messages"})
    void listMessages(HttpServletRequest req, HttpServletResponse res) throws IOException {
        page(req, res, Sql.MESSAGES, false);
    }

    void page(HttpServletRequest req, HttpServletResponse res, String sql, boolean isPost) throws IOException {
        UUID uid = auth(req);
        if (uid == null) { H.fail(res, 401); return; }
        UUID before = H.cursor(req);
        if (before == null) { H.fail(res, 400); return; }
        byte[] b;
        try { b = queryPage(sql, isPost, uid, before); } catch (SQLException e) { H.fail(res, 500); return; }
        H.send(res, 200, b);
    }

    @PostMapping("/posts")
    void createPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        UUID uid = auth(req);
        if (uid == null) { H.fail(res, 401); return; }
        JsonNode o = H.readJson(req, res);
        if (o == null) return;
        NewPost p = H.parsePost(uid, o);
        if (p == null) { H.fail(res, 400); return; }
        H.done(res, posts.submit(p, Batcher.key(uid)));
    }

    @PostMapping("/n/posts")
    void nCreatePost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        UUID uid = auth(req);
        if (uid == null) { H.fail(res, 401); return; }
        JsonNode o = H.readJson(req, res);
        if (o == null) return;
        NewPost p = H.parsePost(uid, o);
        if (p == null) { H.fail(res, 400); return; }
        UUID id = Ids.v7();
        H.done(res, execOne("posts", nPosts, ps -> {
            ps.setObject(1, id);
            ps.setObject(2, p.uid());
            setUuid(ps, 3, p.replyTo());
            ps.setShort(4, p.visibility());
            ps.setString(5, p.title());
            ps.setString(6, p.body());
            ps.setString(7, p.lang());
            ps.setString(8, p.mediaUrl());
        }));
    }

    @PostMapping("/messages")
    void sendMessage(HttpServletRequest req, HttpServletResponse res) throws IOException {
        UUID uid = auth(req);
        if (uid == null) { H.fail(res, 401); return; }
        JsonNode o = H.readJson(req, res);
        if (o == null) return;
        NewMessage m = H.parseMessage(uid, o);
        if (m == null) { H.fail(res, 400); return; }
        // symmetric key: both directions of a conversation share one lane
        H.done(res, messages.submit(m, Batcher.key(uid) ^ Batcher.key(m.to())));
    }

    @PostMapping("/n/messages")
    void nSendMessage(HttpServletRequest req, HttpServletResponse res) throws IOException {
        UUID uid = auth(req);
        if (uid == null) { H.fail(res, 401); return; }
        JsonNode o = H.readJson(req, res);
        if (o == null) return;
        NewMessage m = H.parseMessage(uid, o);
        if (m == null) { H.fail(res, 400); return; }
        UUID id = Ids.v7(), conv = Ids.v7();
        H.done(res, execOne("messages", nMessages, ps -> {
            ps.setObject(1, id);
            ps.setObject(2, conv);
            ps.setObject(3, m.uid());
            ps.setObject(4, m.to());
            ps.setShort(5, m.contentType());
            ps.setString(6, m.body());
            ps.setString(7, m.attachment());
        }));
    }

    @PostMapping("/posts/{id}/like")
    void like(@PathVariable("id") String id, HttpServletRequest req, HttpServletResponse res) throws IOException {
        UUID uid = auth(req);
        if (uid == null) { H.fail(res, 401); return; }
        UUID pid = H.uuid(id);
        if (pid == null) { H.fail(res, 400); return; }
        H.done(res, likes.submit(new NewLike(uid, pid), Batcher.key(pid)));
    }

    @PostMapping("/n/posts/{id}/like")
    void nLike(@PathVariable("id") String id, HttpServletRequest req, HttpServletResponse res) throws IOException {
        UUID uid = auth(req);
        if (uid == null) { H.fail(res, 401); return; }
        UUID pid = H.uuid(id);
        if (pid == null) { H.fail(res, 400); return; }
        UUID lid = Ids.v7();
        H.done(res, execOne("likes", nLikes, ps -> {
            ps.setObject(1, lid);
            ps.setObject(2, uid);
            ps.setObject(3, pid);
        }));
    }
}

// ---------- SQL (identical in every app; $n -> ? for JDBC, params are used once and in order) ----------
final class Sql {
    static String jdbc(String s) { return s.replaceAll("\\$\\d+", "?"); }

    static final String POST_SELECT =
        "SELECT p.id, p.user_id, u.username, u.display_name, u.avatar_url, u.is_verified, p.reply_to_id, " +
        "p.title, left(p.body, 210) AS preview, p.lang, p.media_url, p.like_count, p.comment_count, " +
        "p.share_count, (extract(epoch FROM p.created_at)*1000)::int8 AS created_ms " +
        "FROM posts p JOIN users u ON u.id = p.user_id ";
    static final String PUBLIC = jdbc(POST_SELECT + "WHERE p.visibility = 0 AND p.deleted_at IS NULL AND p.id < $1 ORDER BY p.id DESC LIMIT 5");
    static final String PRIVATE = jdbc(POST_SELECT + "WHERE p.user_id = $1 AND p.visibility = 1 AND p.deleted_at IS NULL AND p.id < $2 ORDER BY p.id DESC LIMIT 5");
    static final String MESSAGES = jdbc(
        "SELECT m.id, m.conversation_id, m.sender_id, u.username AS sender_username, " +
        "u.display_name AS sender_display_name, u.avatar_url AS sender_avatar_url, m.content_type, " +
        "left(m.body, 240) AS preview, m.attachment_url, (extract(epoch FROM m.created_at)*1000)::int8 AS created_ms, " +
        "(extract(epoch FROM m.read_at)*1000)::int8 AS read_ms " +
        "FROM messages m JOIN users u ON u.id = m.sender_id " +
        "WHERE m.recipient_id = $1 AND m.deleted_at IS NULL AND m.id < $2 ORDER BY m.id DESC LIMIT 5");
    static final String USER_EXISTS = jdbc("SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)");

    static final String POSTS_UNNEST =
        "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int2[], $5::text[], $6::text[], $7::text[], $8::text[]) " +
        "AS t(id, user_id, reply_to_id, visibility, title, body, lang, media_url)";
    static final String MESSAGES_UNNEST =
        "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::int2[], $6::text[], $7::text[]) " +
        "AS t(id, new_conv_id, sender_id, recipient_id, content_type, body, attachment_url)";
    static final String LIKES_UNNEST = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) AS t(id, user_id, post_id)";

    // group 2: single-row scalar sources
    static final String POSTS_ONE =
        "SELECT ?::uuid AS id, ?::uuid AS user_id, ?::uuid AS reply_to_id, ?::int2 AS visibility, " +
        "?::text AS title, ?::text AS body, ?::text AS lang, ?::text AS media_url";
    static final String MESSAGES_ONE =
        "SELECT ?::uuid AS id, ?::uuid AS new_conv_id, ?::uuid AS sender_id, ?::uuid AS recipient_id, " +
        "?::int2 AS content_type, ?::text AS body, ?::text AS attachment_url";
    static final String LIKES_ONE = "SELECT ?::uuid AS id, ?::uuid AS user_id, ?::uuid AS post_id";

    static String posts(String src) {
        return jdbc("WITH raw AS (" + src + "), " +
            "ins AS ( " +
            "INSERT INTO posts (id, user_id, reply_to_id, visibility, title, body, lang, media_url, like_count, comment_count, " +
            "share_count, view_count, is_edited, created_at, updated_at, deleted_at) " +
            "SELECT r.id, r.user_id, r.reply_to_id, r.visibility, r.title, r.body, r.lang, r.media_url, 0, 0, 0, 0, false, " +
            "uuid_extract_timestamp(r.id), uuid_extract_timestamp(r.id), NULL " +
            "FROM raw r " +
            "WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) " +
            "AND (r.reply_to_id IS NULL OR EXISTS (SELECT 1 FROM posts p WHERE p.id = r.reply_to_id)) " +
            "ON CONFLICT (id) DO NOTHING " +
            "RETURNING user_id, reply_to_id), " +
            "by_user AS ( " +
            "UPDATE users SET posts_count = users.posts_count + c.n " +
            "FROM (SELECT user_id, count(*) AS n FROM ins GROUP BY user_id ORDER BY user_id) c " +
            "WHERE users.id = c.user_id) " +
            "UPDATE posts SET comment_count = posts.comment_count + c.n " +
            "FROM (SELECT reply_to_id, count(*) AS n FROM ins WHERE reply_to_id IS NOT NULL GROUP BY reply_to_id ORDER BY reply_to_id) c " +
            "WHERE posts.id = c.reply_to_id");
    }

    static String messages(String src) {
        return jdbc("WITH raw AS (" + src + "), " +
            "v AS ( " +
            "SELECT r.*, least(r.sender_id, r.recipient_id) AS ua, greatest(r.sender_id, r.recipient_id) AS ub " +
            "FROM raw r " +
            "WHERE r.sender_id <> r.recipient_id " +
            "AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.recipient_id) " +
            "AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.sender_id)), " +
            "last AS ( " +
            "SELECT DISTINCT ON (ua, ub) ua, ub, new_conv_id, id, left(body, 100) AS preview, " +
            "count(*) OVER (PARTITION BY ua, ub) AS n " +
            "FROM v ORDER BY ua, ub, id DESC), " +
            "conv AS ( " +
            "INSERT INTO conversations AS c (id, user_a_id, user_b_id, last_message_id, last_message_preview, " +
            "last_message_at, message_count, created_at, updated_at) " +
            "SELECT new_conv_id, ua, ub, id, preview, uuid_extract_timestamp(id), n, " +
            "uuid_extract_timestamp(new_conv_id), uuid_extract_timestamp(id) " +
            "FROM last " +
            "ON CONFLICT (user_a_id, user_b_id) DO UPDATE SET " +
            "last_message_id = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_id ELSE c.last_message_id END, " +
            "last_message_preview = CASE WHEN EXCLUDED.last_message_id > c.last_message_id THEN EXCLUDED.last_message_preview ELSE c.last_message_preview END, " +
            "last_message_at = greatest(EXCLUDED.last_message_at, c.last_message_at), " +
            "message_count = c.message_count + EXCLUDED.message_count, " +
            "updated_at = greatest(EXCLUDED.updated_at, c.updated_at) " +
            "RETURNING c.id, c.user_a_id, c.user_b_id) " +
            "INSERT INTO messages (id, conversation_id, sender_id, recipient_id, content_type, body, attachment_url, " +
            "created_at, read_at, edited_at, deleted_at) " +
            "SELECT v.id, conv.id, v.sender_id, v.recipient_id, v.content_type, v.body, v.attachment_url, " +
            "uuid_extract_timestamp(v.id), NULL, NULL, NULL " +
            "FROM v JOIN conv ON conv.user_a_id = v.ua AND conv.user_b_id = v.ub " +
            "ON CONFLICT (id) DO NOTHING");
    }

    static String likes(String src) {
        return jdbc("WITH raw AS (" + src + "), " +
            "ins AS ( " +
            "INSERT INTO likes (id, user_id, post_id, created_at) " +
            "SELECT r.id, r.user_id, r.post_id, uuid_extract_timestamp(r.id) " +
            "FROM raw r " +
            "WHERE EXISTS (SELECT 1 FROM posts p WHERE p.id = r.post_id) " +
            "AND EXISTS (SELECT 1 FROM users u WHERE u.id = r.user_id) " +
            "ON CONFLICT DO NOTHING " +
            "RETURNING post_id) " +
            "UPDATE posts SET like_count = posts.like_count + c.n " +
            "FROM (SELECT post_id, count(*) AS n FROM ins GROUP BY post_id ORDER BY post_id) c " +
            "WHERE posts.id = c.post_id");
    }
}

// ---------- cache TTL: deterministic hash bucketing ----------
// ttl_ms = MIN + (h % BINS) * W + ((h >> 32) % W), W = (MAX - MIN) / BINS, h = FNV-1a 64("public:" + uuid)
final class HashTtl {
    final long min, bins, w;

    HashTtl(long min, long max, long bins) {
        this.min = min;
        this.bins = bins;
        this.w = Math.max(1, (max - min) / bins);
    }

    long ms(String key) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < 7; i++) { h ^= "public:".charAt(i); h *= 0x100000001b3L; }
        for (int i = 0; i < key.length(); i++) { h ^= key.charAt(i); h *= 0x100000001b3L; } // uuid text is ASCII
        return min + Long.remainderUnsigned(h, bins) * w + Long.remainderUnsigned(h >>> 32, w);
    }
}

// ---------- UUIDv7: 48-bit ms | ver 7 | 12-bit seq | var 10 | 62 random bits ----------
final class Ids {
    private static long lastMs, seq;

    static synchronized UUID v7() {
        long ms = System.currentTimeMillis();
        if (ms <= lastMs) {
            if (++seq > 0xFFF) { lastMs++; seq = 0; }
            ms = lastMs;
        } else {
            lastMs = ms;
            seq = 0;
        }
        long msb = (ms << 16) | 0x7000L | seq;
        long lsb = (ThreadLocalRandom.current().nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb);
    }
}

// ---------- column batches ----------
record NewPost(UUID uid, UUID replyTo, short visibility, String title, String body, String lang, String mediaUrl) {}
record NewMessage(UUID uid, UUID to, short contentType, String body, String attachment) {}
record NewLike(UUID uid, UUID postId) {}

abstract class Table<R> {
    abstract String name();
    abstract String sql();
    // generated ids of a batch (one array per id column); created once per batch, so a retry
    // re-sends the same ids (ON CONFLICT (id) DO NOTHING)
    abstract int idCols();
    abstract void bind(Connection c, PreparedStatement ps, List<R> rows, UUID[][] ids) throws SQLException;

    UUID[][] ids(int n) {
        UUID[][] ids = new UUID[idCols()][n];
        for (UUID[] col : ids) for (int i = 0; i < n; i++) col[i] = Ids.v7();
        return ids;
    }
}

final class PostsTable extends Table<NewPost> {
    final String sql = Sql.posts(Sql.POSTS_UNNEST);
    String name() { return "posts"; }
    String sql() { return sql; }

    int idCols() { return 1; }

    void bind(Connection c, PreparedStatement ps, List<NewPost> rows, UUID[][] ids) throws SQLException {
        int n = rows.size();
        UUID[] id = ids[0], uid = new UUID[n], reply = new UUID[n];
        Short[] vis = new Short[n];
        String[] title = new String[n], body = new String[n], lang = new String[n], media = new String[n];
        for (int i = 0; i < n; i++) {
            NewPost r = rows.get(i);
            uid[i] = r.uid(); reply[i] = r.replyTo(); vis[i] = r.visibility();
            title[i] = r.title(); body[i] = r.body(); lang[i] = r.lang(); media[i] = r.mediaUrl();
        }
        ps.setArray(1, c.createArrayOf("uuid", id));
        ps.setArray(2, c.createArrayOf("uuid", uid));
        ps.setArray(3, c.createArrayOf("uuid", reply));
        ps.setArray(4, c.createArrayOf("int2", vis));
        ps.setArray(5, c.createArrayOf("text", title));
        ps.setArray(6, c.createArrayOf("text", body));
        ps.setArray(7, c.createArrayOf("text", lang));
        ps.setArray(8, c.createArrayOf("text", media));
    }
}

final class MessagesTable extends Table<NewMessage> {
    final String sql = Sql.messages(Sql.MESSAGES_UNNEST);
    String name() { return "messages"; }
    String sql() { return sql; }

    int idCols() { return 2; }

    void bind(Connection c, PreparedStatement ps, List<NewMessage> rows, UUID[][] ids) throws SQLException {
        int n = rows.size();
        UUID[] id = ids[0], conv = ids[1], snd = new UUID[n], rcp = new UUID[n];
        Short[] ct = new Short[n];
        String[] body = new String[n], att = new String[n];
        for (int i = 0; i < n; i++) {
            NewMessage r = rows.get(i);
            snd[i] = r.uid(); rcp[i] = r.to();
            ct[i] = r.contentType(); body[i] = r.body(); att[i] = r.attachment();
        }
        ps.setArray(1, c.createArrayOf("uuid", id));
        ps.setArray(2, c.createArrayOf("uuid", conv));
        ps.setArray(3, c.createArrayOf("uuid", snd));
        ps.setArray(4, c.createArrayOf("uuid", rcp));
        ps.setArray(5, c.createArrayOf("int2", ct));
        ps.setArray(6, c.createArrayOf("text", body));
        ps.setArray(7, c.createArrayOf("text", att));
    }
}

final class LikesTable extends Table<NewLike> {
    final String sql = Sql.likes(Sql.LIKES_UNNEST);
    String name() { return "likes"; }
    String sql() { return sql; }

    int idCols() { return 1; }

    void bind(Connection c, PreparedStatement ps, List<NewLike> rows, UUID[][] ids) throws SQLException {
        int n = rows.size();
        UUID[] id = ids[0], uid = new UUID[n], pid = new UUID[n];
        for (int i = 0; i < n; i++) { uid[i] = rows.get(i).uid(); pid[i] = rows.get(i).postId(); }
        ps.setArray(1, c.createArrayOf("uuid", id));
        ps.setArray(2, c.createArrayOf("uuid", uid));
        ps.setArray(3, c.createArrayOf("uuid", pid));
    }
}

// ---------- batcher: BATCH_LANES bounded queues + writer threads per table ----------
final class Batcher<R> {
    static final int ATTEMPTS = 4; // backoff 50, 100, 200 ms between them

    // the batch window runs from the arrival (nanoTime) of its first row
    record Job<R>(R row, CompletableFuture<Integer> done, long at) {}

    final List<ArrayBlockingQueue<Job<R>>> lanes = new ArrayList<>();
    final AtomicLong statFlushes = new AtomicLong(), statRows = new AtomicLong(), statFlushUs = new AtomicLong(),
        statMaxUs = new AtomicLong(), statRetries = new AtomicLong(), statFails = new AtomicLong();
    final HikariDataSource db;
    final Table<R> t;
    final int max;
    final long windowNs;

    Batcher(HikariDataSource db, Table<R> t, int max, long windowNs, int lanes, int queueMax) {
        this.db = db;
        this.t = t;
        this.max = max;
        this.windowNs = windowNs;
        int perLane = Math.max(1, queueMax / lanes);
        for (int i = 0; i < lanes; i++) {
            ArrayBlockingQueue<Job<R>> q = new ArrayBlockingQueue<>(perLane);
            this.lanes.add(q);
            Thread.ofPlatform().daemon().name("batch-" + t.name() + "-" + i).start(() -> loop(q));
        }
    }

    // lane key: last 4 bytes of a UUID (random part of a v7 id)
    static int key(UUID u) { return (int) u.getLeastSignificantBits(); }

    // blocks the calling (virtual) thread until the batch containing the row commits;
    // full lane queue (WRITE_QUEUE_MAX / BATCH_LANES rows) -> 503 busy
    int submit(R row, int key) {
        Job<R> j = new Job<>(row, new CompletableFuture<>(), System.nanoTime());
        return lanes.get(Integer.remainderUnsigned(key, lanes.size())).offer(j) ? j.done().join() : 503;
    }

    // one upsert statement per batch; ids are generated once by the caller
    void write(List<R> rows, UUID[][] ids) throws SQLException {
        try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(t.sql())) {
            t.bind(c, ps, rows, ids);
            ps.executeUpdate();
        }
    }

    // deadlock, serialization, statement timeout, too many connections, server shutdown,
    // connection failures (class 08) and errors without a SQLSTATE (I/O, pool timeout)
    static boolean retryable(Exception e) {
        if (!(e instanceof SQLException se)) return false;
        String st = se.getSQLState();
        if (st == null || st.startsWith("08")) return true;
        return switch (st) {
            case "40P01", "40001", "57014", "53300", "57P01", "57P02", "57P03" -> true;
            default -> false;
        };
    }

    // transient errors are retried with backoff; after the last attempt the batch fails loudly
    boolean writeRetry(List<R> rows) {
        UUID[][] ids = t.ids(rows.size());
        for (int attempt = 1; ; attempt++) {
            try {
                write(rows, ids);
                return true;
            } catch (Exception e) {
                if (attempt < ATTEMPTS && retryable(e)) {
                    statRetries.incrementAndGet();
                    System.err.println("batch " + t.name() + " attempt " + attempt + " failed, retrying: " + e.getMessage());
                    try { Thread.sleep(50L << (attempt - 1)); } catch (InterruptedException ie) { return false; }
                    continue;
                }
                statFails.incrementAndGet();
                System.err.println("BATCH FAILED " + t.name() + " rows=" + rows.size() + " after " + attempt + " attempt(s): " + e);
                return false;
            }
        }
    }

    void loop(ArrayBlockingQueue<Job<R>> q) {
        List<R> rows = new ArrayList<>(max);
        List<CompletableFuture<Integer>> waiters = new ArrayList<>(max);
        while (true) {
            rows.clear();
            waiters.clear();
            try {
                Job<R> j = q.take();
                rows.add(j.row());
                waiters.add(j.done());
                long deadline = j.at() + windowNs;
                while (rows.size() < max) {
                    long rem = deadline - System.nanoTime();
                    j = rem > 0 ? q.poll(rem, TimeUnit.NANOSECONDS) : q.poll();
                    if (j == null) break;
                    rows.add(j.row());
                    waiters.add(j.done());
                }
            } catch (InterruptedException e) {
                return;
            }
            long t0 = System.nanoTime();
            int code = writeRetry(rows) ? 200 : 500;
            long us = (System.nanoTime() - t0) / 1000;
            statFlushes.incrementAndGet();
            statRows.addAndGet(rows.size());
            statFlushUs.addAndGet(us);
            statMaxUs.accumulateAndGet(us, Math::max);
            // ack only after the transaction committed (or failed for good)
            for (CompletableFuture<Integer> w : waiters) w.complete(code);
        }
    }

    void report() {
        long f = statFlushes.getAndSet(0), r = statRows.getAndSet(0), us = statFlushUs.getAndSet(0), mx = statMaxUs.getAndSet(0);
        long re = statRetries.getAndSet(0), fa = statFails.getAndSet(0);
        int queued = 0;
        for (ArrayBlockingQueue<Job<R>> q : lanes) queued += q.size();
        if (f == 0 && queued == 0) return;
        double fm = Math.max(1, f);
        System.err.println(String.format("batch-stats %s flushes=%d rows=%d avg_rows=%.0f avg_flush_ms=%.1f max_flush_ms=%.1f queued=%d retries=%d fails=%d",
            t.name(), f, r, r / fm, us / fm / 1000, mx / 1000.0, queued, re, fa));
    }
}

// ---------- helpers ----------
final class H {
    static final int BODY_LIMIT = 16 * 1024;
    static final JsonFactory JSON = new JsonFactory();
    static final ObjectMapper MAPPER = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    static final byte[] OK = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
    static final byte[] HEALTH = "{\"ok\":true,\"framework\":\"spring\"}".getBytes(StandardCharsets.UTF_8);
    static final Map<Integer, byte[]> ERRS = Map.of(
        400, "{\"error\":\"bad_request\"}".getBytes(StandardCharsets.UTF_8),
        401, "{\"error\":\"unauthorized\"}".getBytes(StandardCharsets.UTF_8),
        404, "{\"error\":\"not_found\"}".getBytes(StandardCharsets.UTF_8),
        413, "{\"error\":\"too_large\"}".getBytes(StandardCharsets.UTF_8),
        500, "{\"error\":\"db\"}".getBytes(StandardCharsets.UTF_8),
        503, "{\"error\":\"busy\"}".getBytes(StandardCharsets.UTF_8));
    static final UUID UUID_MAX = new UUID(-1L, -1L);

    static void send(HttpServletResponse res, int code, byte[] body) throws IOException {
        res.setStatus(code);
        res.setContentType("application/json");
        res.setContentLength(body.length);
        res.getOutputStream().write(body);
    }

    static void fail(HttpServletResponse res, int code) throws IOException { send(res, code, ERRS.get(code)); }

    static void done(HttpServletResponse res, int code) throws IOException {
        if (code == 200) send(res, 200, OK); else fail(res, code);
    }

    // canonical hyphenated UUID only (36 chars)
    static UUID uuid(String s) {
        if (s == null || s.length() != 36) return null;
        for (int i = 0; i < 36; i++) {
            char c = s.charAt(i);
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (c != '-') return null;
            } else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return null;
            }
        }
        return UUID.fromString(s);
    }

    static UUID cursor(HttpServletRequest req) {
        String q = req.getParameter("before");
        return q == null ? UUID_MAX : uuid(q);
    }

    // length in Unicode code points (same as the other apps)
    static boolean lenOk(String s, int min, int max) {
        if (s.length() < min || s.length() > max * 2) return false;
        int n = s.codePointCount(0, s.length());
        return n >= min && n <= max;
    }

    // null when absent or JSON null
    static JsonNode field(JsonNode o, String k) {
        JsonNode n = o.get(k);
        return n == null || n.isNull() ? null : n;
    }

    // shared POST /posts validation; null -> 400
    static NewPost parsePost(UUID uid, JsonNode o) {
        JsonNode title = field(o, "title"), body = field(o, "body"), vis = field(o, "visibility");
        JsonNode lang = field(o, "lang"), reply = field(o, "reply_to"), media = field(o, "media_url");
        if (title == null || !title.isTextual() || !lenOk(title.textValue(), 1, 200)
            || body == null || !body.isTextual() || !lenOk(body.textValue(), 1, 8000)
            || vis == null || !vis.isIntegralNumber() || !vis.canConvertToInt() || (vis.intValue() != 0 && vis.intValue() != 1)
            || (lang != null && (!lang.isTextual() || !lenOk(lang.textValue(), 2, 8)))
            || (media != null && (!media.isTextual() || !lenOk(media.textValue(), 1, 500)))
            || (reply != null && (!reply.isTextual() || uuid(reply.textValue()) == null))) {
            return null;
        }
        return new NewPost(uid, reply == null ? null : uuid(reply.textValue()), (short) vis.intValue(),
            title.textValue(), body.textValue(), lang == null ? "en" : lang.textValue(), media == null ? null : media.textValue());
    }

    // shared POST /messages validation; null -> 400
    static NewMessage parseMessage(UUID uid, JsonNode o) {
        JsonNode toN = field(o, "to"), body = field(o, "body"), ct = field(o, "content_type"), att = field(o, "attachment_url");
        UUID to = toN != null && toN.isTextual() ? uuid(toN.textValue()) : null;
        if (to == null || to.equals(uid)
            || body == null || !body.isTextual() || !lenOk(body.textValue(), 1, 8000)
            || (ct != null && (!ct.isIntegralNumber() || !ct.canConvertToInt() || ct.intValue() < 0 || ct.intValue() > 3))
            || (att != null && (!att.isTextual() || !lenOk(att.textValue(), 1, 500)))) {
            return null;
        }
        return new NewMessage(uid, to, ct == null ? 0 : (short) ct.intValue(), body.textValue(),
            att == null ? null : att.textValue());
    }

    // reads at most 16 KB; returns the JSON object or writes 400/413 and returns null
    static JsonNode readJson(HttpServletRequest req, HttpServletResponse res) throws IOException {
        long cl = req.getContentLengthLong();
        if (cl > BODY_LIMIT) { fail(res, 413); return null; }
        byte[] buf;
        int n;
        if (cl >= 0) {
            buf = new byte[(int) cl]; // exactly Content-Length bytes
            n = req.getInputStream().readNBytes(buf, 0, buf.length);
        } else {
            buf = req.getInputStream().readNBytes(BODY_LIMIT + 1); // no Content-Length: bounded, grows in chunks
            n = buf.length;
            if (n > BODY_LIMIT) { fail(res, 413); return null; }
        }
        JsonNode o;
        try {
            o = MAPPER.readTree(buf, 0, n);
        } catch (IOException e) {
            fail(res, 400);
            return null;
        }
        if (o == null || !o.isObject()) { fail(res, 400); return null; }
        return o;
    }

    // returns the item id (for "next")
    static String writePost(JsonGenerator w, ResultSet r) throws SQLException, IOException {
        String id = r.getObject(1, UUID.class).toString();
        w.writeStartObject();
        w.writeStringField("id", id);
        w.writeStringField("user_id", r.getObject(2, UUID.class).toString());
        w.writeStringField("username", r.getString(3));
        w.writeStringField("display_name", r.getString(4));
        w.writeStringField("avatar_url", r.getString(5));
        w.writeBooleanField("is_verified", r.getBoolean(6));
        UUID reply = r.getObject(7, UUID.class);
        if (reply == null) w.writeNullField("reply_to_id"); else w.writeStringField("reply_to_id", reply.toString());
        w.writeStringField("title", r.getString(8));
        w.writeStringField("preview", r.getString(9));
        w.writeStringField("lang", r.getString(10));
        w.writeStringField("media_url", r.getString(11));
        w.writeNumberField("like_count", r.getLong(12));
        w.writeNumberField("comment_count", r.getLong(13));
        w.writeNumberField("share_count", r.getLong(14));
        w.writeNumberField("created_ms", r.getLong(15));
        w.writeEndObject();
        return id;
    }

    static String writeMessage(JsonGenerator w, ResultSet r) throws SQLException, IOException {
        String id = r.getObject(1, UUID.class).toString();
        w.writeStartObject();
        w.writeStringField("id", id);
        w.writeStringField("conversation_id", r.getObject(2, UUID.class).toString());
        w.writeStringField("sender_id", r.getObject(3, UUID.class).toString());
        w.writeStringField("sender_username", r.getString(4));
        w.writeStringField("sender_display_name", r.getString(5));
        w.writeStringField("sender_avatar_url", r.getString(6));
        w.writeNumberField("content_type", r.getInt(7));
        w.writeStringField("preview", r.getString(8));
        w.writeStringField("attachment_url", r.getString(9));
        w.writeNumberField("created_ms", r.getLong(10));
        long read = r.getLong(11);
        if (r.wasNull()) w.writeNullField("read_ms"); else w.writeNumberField("read_ms", read);
        w.writeEndObject();
        return id;
    }
}

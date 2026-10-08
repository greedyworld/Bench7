// bench7 — ASP.NET Core minimal API (Kestrel + Npgsql + IMemoryCache + Microsoft.IdentityModel.JsonWebTokens).
// Same endpoints, SQL, cache, batcher and JWT rules as the other apps.
//   - ids: UUIDv7 generated here (Guid.CreateVersion7); created_at = uuid_extract_timestamp(id)
//   - cache TTL: deterministic hash bucketing (spreads expiries, no thundering herd)
//   - writes: per-table batcher, lanes keyed by user / conversation / post -> column arrays ->
//     one upsert transaction per batch (unnest); the reply is sent after commit
using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using Npgsql;
using NpgsqlTypes;

string Env(string k, string d) { var v = Environment.GetEnvironmentVariable(k); return string.IsNullOrEmpty(v) ? d : v; }
int EnvInt(string k, int d) => int.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;

var poolSize = EnvInt("DB_POOL_TOTAL", 12);
var batchCfg = new BatchCfg(
    Math.Max(1, EnvInt("BATCH_MAX_ROWS", 1000)),
    TimeSpan.FromMilliseconds(EnvInt("BATCH_WINDOW_MS", 200)),
    Math.Max(1, EnvInt("BATCH_LANES", 4)),
    EnvInt("WRITE_QUEUE_MAX", 40000),
    Env("BATCH_STATS", "0") == "1");
var stmtTimeout = EnvInt("STATEMENT_TIMEOUT_MS", 5000);
var secret = Env("JWT_SECRET", "");
var issuerKey = Encoding.UTF8.GetBytes(Env("TOKEN_ISSUER_KEY", ""));
var iss = Env("JWT_ISS", "bench7");
var aud = Env("JWT_AUD", "bench7-api");
var jwtTtl = EnvInt("JWT_TTL_S", 3600);
if (secret.Length == 0 || issuerKey.Length == 0)
{
    Console.Error.WriteLine("JWT_SECRET and TOKEN_ISSUER_KEY are required");
    return 1;
}

// ---------- runtime knobs (all default off) ----------
var kestrelInline = Env("KESTREL_INLINE", "0") == "1";
var multiplexing = Env("NPGSQL_MULTIPLEXING", "0") == "1";
var minThreads = EnvInt("MIN_THREADS", 0);
if (minThreads > 0) ThreadPool.SetMinThreads(minThreads, minThreads);

// ---------- database ----------
var dbUri = new Uri(Env("DATABASE_URL", ""));
var userInfo = dbUri.UserInfo.Split(':', 2);
var csb = new NpgsqlConnectionStringBuilder
{
    Host = dbUri.Host,
    Port = dbUri.Port > 0 ? dbUri.Port : 5432,
    Database = dbUri.AbsolutePath.TrimStart('/'),
    Username = Uri.UnescapeDataString(userInfo[0]),
    Password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : null,
    MinPoolSize = poolSize,
    MaxPoolSize = poolSize,
    NoResetOnClose = true, // no DISCARD ALL round trip when a connection returns to the pool
    MaxAutoPrepare = 32,
    AutoPrepareMinUsages = 2,
};
if (multiplexing) csb.Multiplexing = true;
if (stmtTimeout > 0) csb.Options = $"-c statement_timeout={stmtTimeout}";
await using var db = new NpgsqlDataSourceBuilder(csb.ConnectionString).Build();
await using (var probe = await db.OpenConnectionAsync()) { }

// ---------- cache ----------
var cache = new MemoryCache(new MemoryCacheOptions { SizeLimit = (long)EnvInt("CACHE_MAX_MB", 64) << 20 });
var ttlMin = (ulong)EnvInt("CACHE_TTL_MIN_MS", 45000);
var ttlMax = (ulong)EnvInt("CACHE_TTL_MAX_MS", 60000);
var ttl = new HashTtl(ttlMin, ttlMax, (ulong)Math.Max(1, EnvInt("CACHE_TTL_BINS", 15)));
var flights = new ConcurrentDictionary<string, Lazy<Task<byte[]>>>();

// ---------- batchers ----------
var posts = new Batcher<NewPost>(db, new PostsTable(), batchCfg);
var messages = new Batcher<NewMessage>(db, new MessagesTable(), batchCfg);
var likes = new Batcher<NewLike>(db, new LikesTable(), batchCfg);

// group 2 ("normal", prefix /n): same SQL as the batch writers, fed one row from scalar parameters.
var nPostsSql = Sql.Posts(Sql.PostsOne);
var nMessagesSql = Sql.MessagesBatch(Sql.MessagesOne);
var nLikesSql = Sql.Likes(Sql.LikesOne);

// Runs one write statement directly on the data source; retried on deadlock / serialization failure like the batcher.
async Task<int> ExecOne(string name, string sql, Action<NpgsqlParameterCollection> bind)
{
    for (int attempt = 0; attempt < 3; attempt++)
    {
        try
        {
            await using var cmd = db.CreateCommand(sql);
            bind(cmd.Parameters);
            await cmd.ExecuteNonQueryAsync();
            return 200;
        }
        catch (PostgresException e) when (attempt < 2 && e.SqlState is "40P01" or "40001") { }
        catch (Exception e)
        {
            Console.Error.WriteLine($"write {name} failed: {e.Message}");
            break;
        }
    }
    return 500;
}

// ---------- JWT ----------
var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(secret));
var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
var jwtHandler = new JsonWebTokenHandler { SetDefaultTimesOnTokenCreation = false };
var tvp = new TokenValidationParameters
{
    ValidIssuer = iss,
    ValidAudience = aud,
    IssuerSigningKey = key,
    ValidAlgorithms = [SecurityAlgorithms.HmacSha256],
    ClockSkew = TimeSpan.Zero,
    RequireExpirationTime = true,
    RequireSignedTokens = true,
    ValidateLifetime = true,
    ValidateIssuer = true,
    ValidateAudience = true,
};

async Task<Guid?> Auth(HttpContext c)
{
    string? h = c.Request.Headers.Authorization;
    if (h is null || !h.StartsWith("Bearer ", StringComparison.Ordinal)) return null;
    var r = await jwtHandler.ValidateTokenAsync(h[7..], tvp);
    if (!r.IsValid || r.SecurityToken is not JsonWebToken t) return null;
    if (!t.TryGetPayloadValue<long>("iat", out var iat) || iat > DateTimeOffset.UtcNow.ToUnixTimeSeconds()) return null;
    if (!t.TryGetPayloadValue<string>("sub", out var sub)) return null;
    return H.ParseUuid(sub);
}

async Task<byte[]> QueryPage(string sql, bool isPost, Guid a, Guid? b)
{
    await using var cmd = db.CreateCommand(sql);
    cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = a });
    if (b is Guid bv) cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = bv });
    await using var r = await cmd.ExecuteReaderAsync();
    var buf = new ArrayBufferWriter<byte>(8192);
    using (var w = new Utf8JsonWriter(buf, H.JsonOpts))
    {
        w.WriteStartObject();
        w.WriteStartArray("items");
        int n = 0;
        Guid last = default;
        while (await r.ReadAsync())
        {
            if (isPost) H.WritePost(w, r); else H.WriteMessage(w, r);
            last = r.GetGuid(0);
            n++;
        }
        w.WriteEndArray();
        if (n == 5) w.WriteString("next", last); else w.WriteNull("next");
        w.WriteEndObject();
    }
    return buf.WrittenSpan.ToArray();
}

async Task<byte[]> LoadPublic(string k)
{
    var b = await QueryPage(Sql.Public, true, Guid.ParseExact(k, "D"), null);
    cache.Set(k, b, new MemoryCacheEntryOptions { Size = b.Length, AbsoluteExpirationRelativeToNow = ttl.For(k) });
    return b;
}

// ---------- HTTP ----------
var builder = WebApplication.CreateSlimBuilder(args);
builder.Logging.ClearProviders();
builder.WebHost.ConfigureKestrel(o => { o.AddServerHeader = false; o.Limits.MaxRequestBodySize = null; });
if (kestrelInline) builder.WebHost.UseSockets(o => o.UnsafePreferInlineScheduling = true);
var app = builder.Build();

RequestDelegate health = c => H.Send(c, 200, H.Health);
app.MapGet("/health", health);

app.MapPost("/auth/token", (RequestDelegate)(async c =>
{
    var hk = Encoding.UTF8.GetBytes(c.Request.Headers["x-issuer-key"].ToString());
    if (!System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(hk, issuerKey)) { await H.Fail(c, 401); return; }
    var (doc, code) = await H.ReadJson(c);
    if (doc is null) { await H.Fail(c, code); return; }
    using (doc)
    {
        if (H.OptStr(doc.RootElement, "user_id", out var s) != 1 || H.ParseUuid(s) is not Guid uid) { await H.Fail(c, 400); return; }
        bool exists;
        try
        {
            await using var cmd = db.CreateCommand(Sql.UserExists);
            cmd.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = uid });
            exists = (bool)(await cmd.ExecuteScalarAsync())!;
        }
        catch (Exception) { await H.Fail(c, 500); return; }
        if (!exists) { await H.Fail(c, 404); return; }
        var now = DateTime.UtcNow;
        var tok = jwtHandler.CreateToken(new SecurityTokenDescriptor
        {
            Issuer = iss,
            Audience = aud,
            IssuedAt = now,
            Expires = now.AddSeconds(jwtTtl),
            Claims = new Dictionary<string, object> { ["sub"] = uid.ToString() },
            SigningCredentials = creds,
        });
        await H.Send(c, 200, Encoding.UTF8.GetBytes($"{{\"token\":\"{tok}\",\"expires_in\":{jwtTtl}}}"));
    }
}));

app.MapGet("/posts/public", (RequestDelegate)(async c =>
{
    if (H.Cursor(c) is not Guid before) { await H.Fail(c, 400); return; }
    var k = before.ToString();
    if (cache.TryGetValue(k, out byte[]? hit) && hit is not null)
    {
        c.Response.Headers["x-cache"] = "hit";
        await H.Send(c, 200, hit);
        return;
    }
    var lazy = flights.GetOrAdd(k, kk => new Lazy<Task<byte[]>>(() => LoadPublic(kk)));
    byte[] b;
    try { b = await lazy.Value; }
    catch (Exception) { await H.Fail(c, 500); return; }
    finally { flights.TryRemove(KeyValuePair.Create(k, lazy)); }
    c.Response.Headers["x-cache"] = "miss";
    await H.Send(c, 200, b);
}));

RequestDelegate privatePosts = async c =>
{
    if (await Auth(c) is not Guid uid) { await H.Fail(c, 401); return; }
    if (H.Cursor(c) is not Guid before) { await H.Fail(c, 400); return; }
    byte[] b;
    try { b = await QueryPage(Sql.Private, true, uid, before); }
    catch (Exception) { await H.Fail(c, 500); return; }
    await H.Send(c, 200, b);
};
app.MapGet("/posts/private", privatePosts);

RequestDelegate listMessages = async c =>
{
    if (await Auth(c) is not Guid uid) { await H.Fail(c, 401); return; }
    if (H.Cursor(c) is not Guid before) { await H.Fail(c, 400); return; }
    byte[] b;
    try { b = await QueryPage(Sql.Messages, false, uid, before); }
    catch (Exception) { await H.Fail(c, 500); return; }
    await H.Send(c, 200, b);
};
app.MapGet("/messages", listMessages);

app.MapPost("/posts", (RequestDelegate)(async c =>
{
    if (await Auth(c) is not Guid uid) { await H.Fail(c, 401); return; }
    if (await H.ReadPost(c, uid) is not NewPost row) return;
    await H.Done(c, await posts.Submit(row, H.Key(uid)));
}));

app.MapPost("/messages", (RequestDelegate)(async c =>
{
    if (await Auth(c) is not Guid uid) { await H.Fail(c, 401); return; }
    if (await H.ReadMessage(c, uid) is not NewMessage row) return;
    // symmetric key: both directions of a conversation share one lane
    await H.Done(c, await messages.Submit(row, H.Key(uid) ^ H.Key(row.To)));
}));

app.MapPost("/posts/{id}/like", (RequestDelegate)(async c =>
{
    if (await Auth(c) is not Guid uid) { await H.Fail(c, 401); return; }
    if (H.ParseUuid(c.Request.RouteValues["id"] as string) is not Guid pid) { await H.Fail(c, 400); return; }
    await H.Done(c, await likes.Submit(new NewLike(uid, pid), H.Key(pid)));
}));

// ---------- group 2 ("normal", prefix /n): no batching, no cache ----------
app.MapGet("/n/health", health);

app.MapGet("/n/posts/public", (RequestDelegate)(async c =>
{
    if (H.Cursor(c) is not Guid before) { await H.Fail(c, 400); return; }
    byte[] b;
    try { b = await QueryPage(Sql.Public, true, before, null); }
    catch (Exception) { await H.Fail(c, 500); return; }
    await H.Send(c, 200, b);
}));

app.MapGet("/n/posts/private", privatePosts);
app.MapGet("/n/messages", listMessages);

app.MapPost("/n/posts", (RequestDelegate)(async c =>
{
    if (await Auth(c) is not Guid uid) { await H.Fail(c, 401); return; }
    if (await H.ReadPost(c, uid) is not NewPost p) return;
    var id = Guid.CreateVersion7();
    await H.Done(c, await ExecOne("posts", nPostsSql, ps =>
    {
        ps.Add(H.Val(id));
        ps.Add(H.Val(p.Uid));
        ps.Add(H.OptUuid(p.ReplyTo));
        ps.Add(H.Val(p.Visibility));
        ps.Add(H.Val(p.Title));
        ps.Add(H.Val(p.Body));
        ps.Add(H.Val(p.Lang));
        ps.Add(H.OptText(p.MediaUrl));
    }));
}));

app.MapPost("/n/messages", (RequestDelegate)(async c =>
{
    if (await Auth(c) is not Guid uid) { await H.Fail(c, 401); return; }
    if (await H.ReadMessage(c, uid) is not NewMessage m) return;
    var id = Guid.CreateVersion7();
    var conv = Guid.CreateVersion7();
    await H.Done(c, await ExecOne("messages", nMessagesSql, ps =>
    {
        ps.Add(H.Val(id));
        ps.Add(H.Val(conv));
        ps.Add(H.Val(m.Uid));
        ps.Add(H.Val(m.To));
        ps.Add(H.Val(m.ContentType));
        ps.Add(H.Val(m.Body));
        ps.Add(H.OptText(m.Attachment));
    }));
}));

app.MapPost("/n/posts/{id}/like", (RequestDelegate)(async c =>
{
    if (await Auth(c) is not Guid uid) { await H.Fail(c, 401); return; }
    if (H.ParseUuid(c.Request.RouteValues["id"] as string) is not Guid pid) { await H.Fail(c, 400); return; }
    var id = Guid.CreateVersion7();
    await H.Done(c, await ExecOne("likes", nLikesSql, ps =>
    {
        ps.Add(H.Val(id));
        ps.Add(H.Val(uid));
        ps.Add(H.Val(pid));
    }));
}));

Console.WriteLine($"dotnet runtime={System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription} " +
    $"cpus={Environment.ProcessorCount} server_gc={System.Runtime.GCSettings.IsServerGC} " +
    $"gc_latency={System.Runtime.GCSettings.LatencyMode} knobs: kestrel_inline={(kestrelInline ? "on" : "off")} " +
    $"npgsql_multiplexing={(multiplexing ? "on" : "off")} min_threads={(minThreads > 0 ? minThreads.ToString() : "off")}");
Console.WriteLine($"aspnet listening on {Env("ASPNETCORE_URLS", "http://0.0.0.0:8080")} pool={poolSize} " +
    $"batch_rows={batchCfg.MaxRows} batch_window_ms={batchCfg.Window.TotalMilliseconds} lanes={batchCfg.Lanes} queue_max={batchCfg.QueueMax} stmt_timeout_ms={stmtTimeout}");
await app.RunAsync();
return 0;

// ---------- SQL (identical in every app) ----------
static class Sql
{
    const string PostSelect =
        "SELECT p.id, p.user_id, u.username, u.display_name, u.avatar_url, u.is_verified, p.reply_to_id, " +
        "p.title, left(p.body, 210) AS preview, p.lang, p.media_url, p.like_count, p.comment_count, " +
        "p.share_count, (extract(epoch FROM p.created_at)*1000)::int8 AS created_ms " +
        "FROM posts p JOIN users u ON u.id = p.user_id ";
    public const string Public = PostSelect + "WHERE p.visibility = 0 AND p.deleted_at IS NULL AND p.id < $1 ORDER BY p.id DESC LIMIT 5";
    public const string Private = PostSelect + "WHERE p.user_id = $1 AND p.visibility = 1 AND p.deleted_at IS NULL AND p.id < $2 ORDER BY p.id DESC LIMIT 5";
    public const string Messages =
        "SELECT m.id, m.conversation_id, m.sender_id, u.username AS sender_username, " +
        "u.display_name AS sender_display_name, u.avatar_url AS sender_avatar_url, m.content_type, " +
        "left(m.body, 240) AS preview, m.attachment_url, (extract(epoch FROM m.created_at)*1000)::int8 AS created_ms, " +
        "(extract(epoch FROM m.read_at)*1000)::int8 AS read_ms " +
        "FROM messages m JOIN users u ON u.id = m.sender_id " +
        "WHERE m.recipient_id = $1 AND m.deleted_at IS NULL AND m.id < $2 ORDER BY m.id DESC LIMIT 5";
    public const string UserExists = "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)";

    public const string PostsUnnest =
        "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int2[], $5::text[], $6::text[], $7::text[], $8::text[]) " +
        "AS t(id, user_id, reply_to_id, visibility, title, body, lang, media_url)";
    public const string MessagesUnnest =
        "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::int2[], $6::text[], $7::text[]) " +
        "AS t(id, new_conv_id, sender_id, recipient_id, content_type, body, attachment_url)";
    public const string LikesUnnest = "SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[]) AS t(id, user_id, post_id)";

    public const string PostsOne =
        "SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS reply_to_id, $4::int2 AS visibility, " +
        "$5::text AS title, $6::text AS body, $7::text AS lang, $8::text AS media_url";
    public const string MessagesOne =
        "SELECT $1::uuid AS id, $2::uuid AS new_conv_id, $3::uuid AS sender_id, $4::uuid AS recipient_id, " +
        "$5::int2 AS content_type, $6::text AS body, $7::text AS attachment_url";
    public const string LikesOne = "SELECT $1::uuid AS id, $2::uuid AS user_id, $3::uuid AS post_id";

    public static string Posts(string src) =>
        "WITH raw AS (" + src + "), " +
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
        "WHERE posts.id = c.reply_to_id";

    public static string MessagesBatch(string src) =>
        "WITH raw AS (" + src + "), " +
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
        "ON CONFLICT (id) DO NOTHING";

    public static string Likes(string src) =>
        "WITH raw AS (" + src + "), " +
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
        "WHERE posts.id = c.post_id";
}

// ---------- cache TTL: deterministic hash bucketing ----------
// ttl_ms = MIN + (h % BINS) * W + ((h >> 32) % W), W = (MAX - MIN) / BINS, h = FNV-1a 64("public:" + uuid)
sealed class HashTtl(ulong min, ulong max, ulong bins)
{
    readonly ulong _w = Math.Max(1, (max - min) / bins);

    public TimeSpan For(string key)
    {
        ulong h = 0xcbf29ce484222325;
        foreach (var ch in "public:")
        {
            h ^= ch;
            h *= 0x100000001b3;
        }
        foreach (var ch in key) // uuid text is ASCII
        {
            h ^= ch;
            h *= 0x100000001b3;
        }
        return TimeSpan.FromMilliseconds(min + (h % bins) * _w + ((h >> 32) % _w));
    }
}

// ---------- column batches ----------
readonly record struct NewPost(Guid Uid, Guid? ReplyTo, short Visibility, string Title, string Body, string Lang, string? MediaUrl);
readonly record struct NewMessage(Guid Uid, Guid To, short ContentType, string Body, string? Attachment);
readonly record struct NewLike(Guid Uid, Guid PostId);

abstract class Table<R>
{
    public abstract string Name { get; }
    public abstract string Sql { get; }
    // column arrays of a batch; ids are generated here, once per batch, so a retry re-sends the same ids
    public abstract Array[] Columns(List<R> rows);
    public abstract NpgsqlParameter[] Params(Array[] c);

    protected static NpgsqlParameter Arr<T>(T[] v) => new NpgsqlParameter<T[]> { TypedValue = v };
}

sealed class PostsTable : Table<NewPost>
{
    public override string Name => "posts";
    public override string Sql { get; } = global::Sql.Posts(global::Sql.PostsUnnest);

    public override Array[] Columns(List<NewPost> rows)
    {
        int n = rows.Count;
        var id = new Guid[n]; var uid = new Guid[n]; var reply = new Guid?[n]; var vis = new short[n];
        var title = new string[n]; var body = new string[n]; var lang = new string[n]; var media = new string?[n];
        for (int i = 0; i < n; i++)
        {
            var r = rows[i];
            id[i] = Guid.CreateVersion7(); uid[i] = r.Uid; reply[i] = r.ReplyTo; vis[i] = r.Visibility;
            title[i] = r.Title; body[i] = r.Body; lang[i] = r.Lang; media[i] = r.MediaUrl;
        }
        return [id, uid, reply, vis, title, body, lang, media];
    }

    public override NpgsqlParameter[] Params(Array[] c) =>
        [Arr((Guid[])c[0]), Arr((Guid[])c[1]), Arr((Guid?[])c[2]), Arr((short[])c[3]),
         Arr((string[])c[4]), Arr((string[])c[5]), Arr((string[])c[6]), Arr((string?[])c[7])];
}

sealed class MessagesTable : Table<NewMessage>
{
    public override string Name => "messages";
    public override string Sql { get; } = global::Sql.MessagesBatch(global::Sql.MessagesUnnest);

    public override Array[] Columns(List<NewMessage> rows)
    {
        int n = rows.Count;
        var id = new Guid[n]; var conv = new Guid[n]; var snd = new Guid[n]; var rcp = new Guid[n];
        var ct = new short[n]; var body = new string[n]; var att = new string?[n];
        for (int i = 0; i < n; i++)
        {
            var r = rows[i];
            id[i] = Guid.CreateVersion7(); conv[i] = Guid.CreateVersion7(); snd[i] = r.Uid; rcp[i] = r.To;
            ct[i] = r.ContentType; body[i] = r.Body; att[i] = r.Attachment;
        }
        return [id, conv, snd, rcp, ct, body, att];
    }

    public override NpgsqlParameter[] Params(Array[] c) =>
        [Arr((Guid[])c[0]), Arr((Guid[])c[1]), Arr((Guid[])c[2]), Arr((Guid[])c[3]),
         Arr((short[])c[4]), Arr((string[])c[5]), Arr((string?[])c[6])];
}

sealed class LikesTable : Table<NewLike>
{
    public override string Name => "likes";
    public override string Sql { get; } = global::Sql.Likes(global::Sql.LikesUnnest);

    public override Array[] Columns(List<NewLike> rows)
    {
        int n = rows.Count;
        var id = new Guid[n]; var uid = new Guid[n]; var pid = new Guid[n];
        for (int i = 0; i < n; i++) { id[i] = Guid.CreateVersion7(); uid[i] = rows[i].Uid; pid[i] = rows[i].PostId; }
        return [id, uid, pid];
    }

    public override NpgsqlParameter[] Params(Array[] c) => [Arr((Guid[])c[0]), Arr((Guid[])c[1]), Arr((Guid[])c[2])];
}

// ---------- batcher: BATCH_LANES queues + writer loops per table ----------
readonly record struct BatchCfg(int MaxRows, TimeSpan Window, int Lanes, int QueueMax, bool Stats);

sealed class Batcher<R>
{
    const int Attempts = 4; // backoff 50, 100, 200 ms between them
    // the batch window runs from the arrival (At, ms) of its first row
    readonly Channel<(R Row, TaskCompletionSource<int> Done, long At)>[] _lanes;
    readonly NpgsqlDataSource _db;
    readonly Table<R> _t;
    readonly int _max;
    readonly long _windowMs;
    long _flushes, _rows, _flushUs, _maxUs, _retries, _fails;

    public Batcher(NpgsqlDataSource db, Table<R> t, BatchCfg cfg)
    {
        (_db, _t, _max, _windowMs) = (db, t, cfg.MaxRows, (long)cfg.Window.TotalMilliseconds);
        var perLane = Math.Max(1, cfg.QueueMax / cfg.Lanes);
        _lanes = new Channel<(R, TaskCompletionSource<int>, long)>[cfg.Lanes];
        for (int i = 0; i < cfg.Lanes; i++)
        {
            var ch = Channel.CreateBounded<(R, TaskCompletionSource<int>, long)>(
                new BoundedChannelOptions(perLane) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
            _lanes[i] = ch;
            _ = Task.Run(() => Loop(ch.Reader));
        }
        if (cfg.Stats) _ = Task.Run(Report);
    }

    // full lane queue -> 503 busy
    public Task<int> Submit(R row, uint key)
    {
        var done = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        return _lanes[key % (uint)_lanes.Length].Writer.TryWrite((row, done, Environment.TickCount64)) ? done.Task : Task.FromResult(503);
    }

    async Task Write(Array[] cols)
    {
        await using var conn = await _db.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(_t.Sql, conn);
        cmd.Parameters.AddRange(_t.Params(cols));
        await cmd.ExecuteNonQueryAsync();
    }

    // deadlock, serialization, statement timeout, too many connections, server shutdown, connection
    // failures (class 08) and I/O / timeout errors without a SQLSTATE
    static bool Retryable(Exception e) => e switch
    {
        PostgresException pe => pe.SqlState is "40P01" or "40001" or "57014" or "53300" or "57P01" or "57P02" or "57P03"
                                || pe.SqlState.StartsWith("08", StringComparison.Ordinal),
        NpgsqlException or TimeoutException or IOException or System.Net.Sockets.SocketException => true,
        _ => false,
    };

    // one upsert transaction per batch; transient errors retried with backoff, then fail loudly
    async Task<bool> WriteRetry(List<R> rows)
    {
        var cols = _t.Columns(rows);
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await Write(cols);
                return true;
            }
            catch (Exception e) when (attempt < Attempts && Retryable(e))
            {
                Interlocked.Increment(ref _retries);
                Console.Error.WriteLine($"batch {_t.Name} attempt {attempt} failed, retrying: {e.Message}");
                await Task.Delay(50 << (attempt - 1));
            }
            catch (Exception e)
            {
                Interlocked.Increment(ref _fails);
                Console.Error.WriteLine($"BATCH FAILED {_t.Name} rows={rows.Count} after {attempt} attempt(s): {e.Message}");
                return false;
            }
        }
    }

    async Task Loop(ChannelReader<(R Row, TaskCompletionSource<int> Done, long At)> reader)
    {
        var rows = new List<R>(_max);
        var waiters = new List<TaskCompletionSource<int>>(_max);
        Task<bool>? wait = null;
        while (true)
        {
            if (!reader.TryRead(out var j))
            {
                if (wait is not null) { await wait; wait = null; }
                else await reader.WaitToReadAsync();
                continue;
            }
            rows.Clear(); waiters.Clear();
            rows.Add(j.Row); waiters.Add(j.Done);
            var delay = Task.Delay(TimeSpan.FromMilliseconds(Math.Max(0, j.At + _windowMs - Environment.TickCount64)));
            while (rows.Count < _max)
            {
                if (reader.TryRead(out j)) { rows.Add(j.Row); waiters.Add(j.Done); continue; }
                wait ??= reader.WaitToReadAsync().AsTask();
                if (await Task.WhenAny(wait, delay) == delay) break;
                wait = null;
            }
            var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
            int code = await WriteRetry(rows) ? 200 : 500;
            var us = (long)System.Diagnostics.Stopwatch.GetElapsedTime(t0).TotalMicroseconds;
            Interlocked.Increment(ref _flushes);
            Interlocked.Add(ref _rows, rows.Count);
            Interlocked.Add(ref _flushUs, us);
            for (long m = Interlocked.Read(ref _maxUs); us > m && Interlocked.CompareExchange(ref _maxUs, us, m) != m; m = Interlocked.Read(ref _maxUs)) { }
            // ack only after the transaction committed (or failed for good)
            foreach (var w in waiters) w.TrySetResult(code);
        }
    }

    async Task Report()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10));
        while (await timer.WaitForNextTickAsync())
        {
            long f = Interlocked.Exchange(ref _flushes, 0), rows = Interlocked.Exchange(ref _rows, 0);
            long us = Interlocked.Exchange(ref _flushUs, 0), mx = Interlocked.Exchange(ref _maxUs, 0);
            long re = Interlocked.Exchange(ref _retries, 0), fa = Interlocked.Exchange(ref _fails, 0);
            int queued = 0;
            foreach (var ch in _lanes) queued += ch.Reader.Count;
            if (f > 0 || queued > 0)
            {
                double fm = Math.Max(1, f);
                Console.Error.WriteLine($"batch-stats {_t.Name} flushes={f} rows={rows} avg_rows={rows / fm:F0} avg_flush_ms={us / fm / 1000:F1} " +
                    $"max_flush_ms={mx / 1000.0:F1} queued={queued} retries={re} fails={fa}");
            }
        }
    }
}

// ---------- helpers ----------
static class H
{
    const int BodyLimit = 16 * 1024;
    static readonly byte[] Ok = "{\"ok\":true}"u8.ToArray();
    public static readonly byte[] Health = "{\"ok\":true,\"framework\":\"aspnet\"}"u8.ToArray();
    static readonly Dictionary<int, byte[]> Errs = new()
    {
        [400] = "{\"error\":\"bad_request\"}"u8.ToArray(),
        [401] = "{\"error\":\"unauthorized\"}"u8.ToArray(),
        [404] = "{\"error\":\"not_found\"}"u8.ToArray(),
        [413] = "{\"error\":\"too_large\"}"u8.ToArray(),
        [500] = "{\"error\":\"db\"}"u8.ToArray(),
        [503] = "{\"error\":\"busy\"}"u8.ToArray(),
    };
    public static readonly JsonWriterOptions JsonOpts = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping, SkipValidation = true };
    static readonly Guid UuidMax = Guid.ParseExact("ffffffff-ffff-ffff-ffff-ffffffffffff", "D");

    public static Task Send(HttpContext c, int code, byte[] body)
    {
        var r = c.Response;
        r.StatusCode = code;
        r.ContentType = "application/json";
        r.ContentLength = body.Length;
        return r.Body.WriteAsync(body).AsTask();
    }

    public static Task Fail(HttpContext c, int code) => Send(c, code, Errs[code]);
    public static Task Done(HttpContext c, int code) => code == 200 ? Send(c, 200, Ok) : Fail(c, code);

    // batch lane key: last 4 bytes of the UUID (random part of a v7 id)
    public static uint Key(Guid g)
    {
        Span<byte> b = stackalloc byte[16];
        g.TryWriteBytes(b);
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(b[12..]);
    }

    // canonical hyphenated UUID only (36 chars)
    public static Guid? ParseUuid(string? s) => s is { Length: 36 } && Guid.TryParseExact(s, "D", out var g) ? g : null;

    public static Guid? Cursor(HttpContext c)
    {
        var q = c.Request.Query["before"];
        return q.Count == 0 ? UuidMax : ParseUuid(q[0]);
    }

    // length in Unicode code points (same as the other apps)
    public static bool LenOk(string s, int min, int max)
    {
        if (s.Length < min || s.Length > max * 2) return false;
        int n = s.Length;
        foreach (var ch in s) if (char.IsLowSurrogate(ch)) n--;
        return n >= min && n <= max;
    }

    // 0 = absent/null, 1 = string, -1 = wrong type
    public static int OptStr(JsonElement o, string k, out string? v)
    {
        v = null;
        if (!o.TryGetProperty(k, out var e) || e.ValueKind == JsonValueKind.Null) return 0;
        if (e.ValueKind != JsonValueKind.String) return -1;
        v = e.GetString();
        return 1;
    }

    // 0 = absent/null, 1 = integer that fits int16, -1 = anything else (no coercion)
    public static int OptInt(JsonElement o, string k, out short v)
    {
        v = 0;
        if (!o.TryGetProperty(k, out var e) || e.ValueKind == JsonValueKind.Null) return 0;
        return e.ValueKind == JsonValueKind.Number && e.TryGetInt16(out v) ? 1 : -1;
    }

    // reads at most 16 KB; returns (doc, 0) or (null, http_code)
    public static async Task<(JsonDocument?, int)> ReadJson(HttpContext c)
    {
        if (c.Request.ContentLength > BodyLimit) return (null, 413);
        var buf = ArrayPool<byte>.Shared.Rent(BodyLimit + 1);
        try
        {
            int n = 0;
            while (true)
            {
                int r = await c.Request.Body.ReadAsync(buf.AsMemory(n, BodyLimit + 1 - n));
                if (r == 0) break;
                n += r;
                if (n > BodyLimit) return (null, 413);
            }
            var doc = ParseObject(buf.AsSpan(0, n));
            if (doc is null) return (null, 400);
            return (doc, 0);
        }
        catch (JsonException) { return (null, 400); }
        catch (BadHttpRequestException) { return (null, 400); }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    // Parses straight from the rented buffer (no ToArray copy); the document keeps its own pooled copy,
    // so the caller's buffer can be returned right away. Rejects trailing data like JsonDocument.Parse did.
    static JsonDocument? ParseObject(ReadOnlySpan<byte> data)
    {
        var reader = new Utf8JsonReader(data);
        var doc = JsonDocument.ParseValue(ref reader);
        bool trailing;
        try { trailing = reader.Read(); }
        catch (JsonException) { doc.Dispose(); throw; }
        if (trailing || doc.RootElement.ValueKind != JsonValueKind.Object) { doc.Dispose(); return null; }
        return doc;
    }

    // null = invalid post body
    public static NewPost? ParsePost(JsonElement o, Guid uid)
    {
        var langK = OptStr(o, "lang", out var lang);
        var replyK = OptStr(o, "reply_to", out var reply);
        Guid? replyTo = null;
        if (OptStr(o, "title", out var title) != 1 || OptStr(o, "body", out var body) != 1
            || OptInt(o, "visibility", out var vis) != 1 || (vis != 0 && vis != 1)
            || langK == -1 || OptStr(o, "media_url", out var media) == -1 || replyK == -1
            || !LenOk(title!, 1, 200) || !LenOk(body!, 1, 8000) || !LenOk(lang ?? "en", 2, 8)
            || (media is not null && !LenOk(media, 1, 500))
            || (reply is not null && (replyTo = ParseUuid(reply)) is null))
            return null;
        return new NewPost(uid, replyTo, vis, title!, body!, lang ?? "en", media);
    }

    // null = invalid message body
    public static NewMessage? ParseMessage(JsonElement o, Guid uid)
    {
        var ctK = OptInt(o, "content_type", out var ct);
        if (OptStr(o, "to", out var toS) != 1 || ParseUuid(toS) is not Guid to || to == uid
            || OptStr(o, "body", out var body) != 1 || !LenOk(body!, 1, 8000)
            || ctK == -1 || ct < 0 || ct > 3
            || OptStr(o, "attachment_url", out var att) == -1 || (att is not null && !LenOk(att, 1, 500)))
            return null;
        return new NewMessage(uid, to, ct, body!, att);
    }

    // read + validate; on failure the error response has already been sent and null is returned
    public static async Task<NewPost?> ReadPost(HttpContext c, Guid uid)
    {
        var (doc, code) = await ReadJson(c);
        if (doc is null) { await Fail(c, code); return null; }
        NewPost? row;
        using (doc) row = ParsePost(doc.RootElement, uid);
        if (row is null) await Fail(c, 400);
        return row;
    }

    public static async Task<NewMessage?> ReadMessage(HttpContext c, Guid uid)
    {
        var (doc, code) = await ReadJson(c);
        if (doc is null) { await Fail(c, code); return null; }
        NewMessage? row;
        using (doc) row = ParseMessage(doc.RootElement, uid);
        if (row is null) await Fail(c, 400);
        return row;
    }

    public static NpgsqlParameter Val<T>(T v) => new NpgsqlParameter<T> { TypedValue = v };
    public static NpgsqlParameter OptUuid(Guid? v) => new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = (object?)v ?? DBNull.Value };
    public static NpgsqlParameter OptText(string? v) => new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)v ?? DBNull.Value };

    public static void WritePost(Utf8JsonWriter w, NpgsqlDataReader r)
    {
        w.WriteStartObject();
        w.WriteString("id", r.GetGuid(0));
        w.WriteString("user_id", r.GetGuid(1));
        w.WriteString("username", r.GetString(2));
        w.WriteString("display_name", r.GetString(3));
        if (r.IsDBNull(4)) w.WriteNull("avatar_url"); else w.WriteString("avatar_url", r.GetString(4));
        w.WriteBoolean("is_verified", r.GetBoolean(5));
        if (r.IsDBNull(6)) w.WriteNull("reply_to_id"); else w.WriteString("reply_to_id", r.GetGuid(6));
        w.WriteString("title", r.GetString(7));
        w.WriteString("preview", r.GetString(8));
        w.WriteString("lang", r.GetString(9));
        if (r.IsDBNull(10)) w.WriteNull("media_url"); else w.WriteString("media_url", r.GetString(10));
        w.WriteNumber("like_count", r.GetInt64(11));
        w.WriteNumber("comment_count", r.GetInt64(12));
        w.WriteNumber("share_count", r.GetInt64(13));
        w.WriteNumber("created_ms", r.GetInt64(14));
        w.WriteEndObject();
    }

    public static void WriteMessage(Utf8JsonWriter w, NpgsqlDataReader r)
    {
        w.WriteStartObject();
        w.WriteString("id", r.GetGuid(0));
        w.WriteString("conversation_id", r.GetGuid(1));
        w.WriteString("sender_id", r.GetGuid(2));
        w.WriteString("sender_username", r.GetString(3));
        w.WriteString("sender_display_name", r.GetString(4));
        if (r.IsDBNull(5)) w.WriteNull("sender_avatar_url"); else w.WriteString("sender_avatar_url", r.GetString(5));
        w.WriteNumber("content_type", r.GetInt16(6));
        w.WriteString("preview", r.GetString(7));
        if (r.IsDBNull(8)) w.WriteNull("attachment_url"); else w.WriteString("attachment_url", r.GetString(8));
        w.WriteNumber("created_ms", r.GetInt64(9));
        if (r.IsDBNull(10)) w.WriteNull("read_ms"); else w.WriteNumber("read_ms", r.GetInt64(10));
        w.WriteEndObject();
    }
}

# Redis key namespace and TTL policy

Owner: Kshiti · Status: locked for Phase 1
Consumers: Pranav (semantic cache), Sriniketh (Phase 2 sessions)

One Redis instance is shared across the platform. Every key MUST carry one of
the prefixes below. Writing an unprefixed key is a bug.

| Prefix   | Owner   | Contents                         | TTL      | Safe to evict? |
|----------|---------|----------------------------------|----------|----------------|
| `cache:` | Pranav  | RedisVL semantic cache entries   | 15 min   | Yes — a miss just costs an LLM call |
| `rl:`    | Kshiti  | Sliding-window rate counters     | = window | No — eviction resets a quota |
| `sess:`  | RESERVED| LynxAuth sessions (Phase 2)      | TBD      | No — eviction logs a user out |
| `cb:`    | Kshiti  | Circuit breaker state per source | 60s      | No |

## Key shapes

- `cache:rag:v1:{sha256(normalized_query + filters + retrieval_context)}`
- `rl:api:{client_ip}`
- `rl:auth:login:{account_id}` and `rl:auth:ip:{client_ip}`   (Phase 2)
- `sess:{opaque_session_id}`                                   (Phase 2)
- `cb:source:{source_id}`

## Rules

1. Version your cache keys (`v1`). Changing the embedding model or the prompt
   changes what a query *means* without changing its text — bump the version
   rather than hoping for a natural expiry.
2. Never run `KEYS *` or `FLUSHDB` in any environment. Use `SCAN` with a prefix.
3. Cache invalidation on supersession is active, not TTL-based. When the Indexer
   emits a supersession event, the RAG API evicts matching `cache:` keys.
   Waiting for TTL means serving a stale deadline for up to 15 minutes.
4. `rl:` and `sess:` must not be evictable. Our dev Redis runs `allkeys-lru`,
   which is wrong for these two namespaces; in production they move to a
   separate Redis DB with `noeviction`.
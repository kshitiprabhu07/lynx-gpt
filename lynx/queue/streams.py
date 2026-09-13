import json
import time
import redis

STREAM = "q:events"
DLQ = "q:events:dead"
MAX_ATTEMPTS = 3


class WorkQueue:
    def __init__(self, redis_url: str, group: str, consumer: str):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)
        self.group = group
        self.consumer = consumer
        try:
            self.r.xgroup_create(STREAM, group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def publish(self, event_type: str, payload: dict) -> str:
        return self.r.xadd(STREAM, {
            "type": event_type,
            "payload": json.dumps(payload),
            "published_at": str(time.time()),
        })

    def consume(self, handler, count: int = 10, block_ms: int = 2000) -> int:
        """Read new messages. Ack on success; leave pending on failure."""
        resp = self.r.xreadgroup(
            self.group, self.consumer, {STREAM: ">"}, count=count, block=block_ms
        )
        handled = 0
        for _stream, messages in resp or []:
            for msg_id, fields in messages:
                try:
                    handler(fields)
                    self.r.xack(STREAM, self.group, msg_id)
                    handled += 1
                except Exception:
                    pass          # stays pending -> picked up by reclaim()
        return handled

    def reclaim(self, handler, min_idle_ms: int = 30_000):
        """Retry stuck messages. Dead-letter past MAX_ATTEMPTS."""
        _cursor, claimed, _ = self.r.xautoclaim(
            STREAM, self.group, self.consumer,
            min_idle_time=min_idle_ms, start_id="0", count=10,
        )
        for msg_id, fields in claimed:
            if not fields:
                self.r.xack(STREAM, self.group, msg_id)
                continue
            pending = self.r.xpending_range(
                STREAM, self.group, min=msg_id, max=msg_id, count=1
            )
            attempts = pending[0]["times_delivered"] if pending else 1

            if attempts > MAX_ATTEMPTS:
                self.r.xadd(DLQ, {**fields, "attempts": attempts,
                                  "failed_at": str(time.time())})
                self.r.xack(STREAM, self.group, msg_id)
                continue
            try:
                handler(fields)
                self.r.xack(STREAM, self.group, msg_id)
            except Exception:
                pass

    def depth(self) -> dict:
        """Operator signal: backlog and oldest message age."""
        pending = self.r.xpending(STREAM, self.group)
        oldest_age = 0.0
        if pending and pending["min"]:
            oldest_ms = int(str(pending["min"]).split("-")[0])
            oldest_age = time.time() - (oldest_ms / 1000)
        return {
            "stream_length": self.r.xlen(STREAM),
            "pending": pending["pending"] if pending else 0,
            "oldest_pending_age_s": round(oldest_age, 1),
            "dead_letter_depth": self.r.xlen(DLQ),
        }
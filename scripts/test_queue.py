from lynx.config import REDIS_URL
from lynx.queue.streams import WorkQueue

q = WorkQueue(REDIS_URL, group="indexer", consumer="worker-1")

print("--- publish three events ---")
q.publish("notice.changed", {"event_id": "e1", "entity_id": "n1", "target_version": 1})
q.publish("notice.changed", {"event_id": "e2", "entity_id": "n2", "target_version": 1})
q.publish("job.changed",    {"event_id": "e3", "entity_id": "j1", "target_version": 1})

seen = []
print("handled:", q.consume(lambda f: seen.append(f["type"])))
print("seen:", seen)
print("depth:", q.depth())

print("\n--- poison event: handler always fails ---")
q.publish("notice.changed", {"event_id": "bad", "entity_id": "n9", "target_version": 1})


def always_fails(fields):
    raise RuntimeError("indexer down")


q.consume(always_fails)
for _ in range(4):
    q.reclaim(always_fails, min_idle_ms=0)

print("final depth:", q.depth())
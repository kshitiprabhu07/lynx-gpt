import time
from lynx.observability.tracing import setup_tracing

tracer = setup_tracing(service_name="lynx-ingestion")

with tracer.start_as_current_span("fetch_run") as root:
    root.set_attribute("source.id", "nitt_notices")
    root.set_attribute("correlation.id", "run_456")
    time.sleep(0.05)

    with tracer.start_as_current_span("change_detect") as s:
        s.set_attribute("material_change", True)
        s.set_attribute("hash_layer", "canonical")
        time.sleep(0.03)

    with tracer.start_as_current_span("index") as s:
        s.set_attribute("chunks", 12)
        time.sleep(0.08)

    with tracer.start_as_current_span("notify") as s:
        s.set_attribute("subscribers_matched", 3)
        time.sleep(0.02)

print("trace sent — open http://localhost:16686")
time.sleep(3)   # let the batch processor flush before the process exits
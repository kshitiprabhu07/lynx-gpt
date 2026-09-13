from lynx.security.ssrf import (
    BlockedRequest, assert_url_is_safe, assert_response_is_safe,
)

ALLOWED = {"www.nitt.edu", "careers.example.com"}

print("--- URL guard ---")
for url in [
    "http://169.254.169.254/latest/meta-data/",   # cloud metadata endpoint
    "http://localhost:6379/",                     # our own Redis
    "file:///etc/passwd",                         # bad scheme
    "https://evil.example.org/x",                 # not allow-listed
]:
    try:
        assert_url_is_safe(url, ALLOWED)
        print(f"ALLOWED (BAD!): {url}")
    except BlockedRequest as e:
        print(f"blocked: {url}\n         -> {e}")

print("\n--- allow-listed host that resolves to loopback ---")
try:
    assert_url_is_safe("http://localhost:6379/", ALLOWED | {"localhost"})
    print("ALLOWED (BAD!)")
except BlockedRequest as e:
    print(f"blocked even though allow-listed: {e}")

print("\n--- response guard ---")
for headers, body, label in [
    ({"content-type": "application/zip"}, b"x", "wrong MIME"),
    ({"content-type": "text/html"}, b"x" * (6 * 1024 * 1024), "oversized body"),
]:
    try:
        assert_response_is_safe(headers, body)
        print(f"ALLOWED (BAD!): {label}")
    except BlockedRequest as e:
        print(f"blocked: {e}")

print("ok:", assert_response_is_safe({"content-type": "text/html"}, b"<html>"))
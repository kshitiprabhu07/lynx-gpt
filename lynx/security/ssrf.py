import ipaddress
import socket
from urllib.parse import urlparse

MAX_BYTES = 5 * 1024 * 1024
MAX_REDIRECTS = 3
ALLOWED_MIME = {"text/html", "application/json",
                "application/ld+json", "application/pdf"}


class BlockedRequest(Exception):
    pass


def assert_url_is_safe(url: str, allowed_hosts: set) -> bool:
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise BlockedRequest(f"scheme not allowed: {parsed.scheme}")

    host = parsed.hostname
    if host is None:
        raise BlockedRequest("no host in URL")
    if host not in allowed_hosts:
        raise BlockedRequest(f"host not in source allow-list: {host}")

    # Check EVERY resolved address, not just the first.
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        raise BlockedRequest(f"DNS resolution failed: {e}")

    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_reserved or ip.is_multicast):
            raise BlockedRequest(f"host resolves to non-public address: {ip}")
    return True


def assert_response_is_safe(headers: dict, body: bytes) -> bool:
    mime = headers.get("content-type", "").split(";")[0].strip().lower()
    if mime not in ALLOWED_MIME:
        raise BlockedRequest(f"MIME type not allowed: {mime!r}")
    if len(body) > MAX_BYTES:
        raise BlockedRequest(f"body too large: {len(body)} bytes")
    return True
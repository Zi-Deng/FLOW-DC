"""Predetermined localhost response policies; no inference from client output."""

from dataclasses import asdict, dataclass

CASES = ("primary", "http-failure", "empty", "retry", "deadline")


@dataclass(frozen=True)
class Response:
    status: int = 200
    retry_after: str | None = None
    first_byte_delay: float = 0
    truncate: bool = False
    empty: bool = False


def case_plan(name, payloads):
    """Policies and original-object truth exist before either client launches."""
    if name not in CASES:
        raise ValueError("unknown engineering case")
    objects = {
        "/left/same.jpg": payloads["JPEG"],
        "/right/same.jpg": payloads["PNG"],
        "/alias.png": payloads["JPEG"],
        "/plain.png": payloads["PNG"],
    }
    policies = {path: [Response()] for path in objects}
    paths = [*objects, "/left/same.jpg", "/right/same.jpg"]
    expected_success = set(objects)
    attempts, request_timeout, process_deadline = 1, 5, 180
    if name == "http-failure":
        extra = {
            "/missing.jpg": Response(404),
            "/busy.jpg": Response(429, "0.05"),
            "/unavailable.jpg": Response(503, "0.05"),
            "/truncated.jpg": Response(truncate=True),
            "/delayed.jpg": Response(first_byte_delay=2),
        }
        objects.update({path: payloads["JPEG"] for path in extra})
        policies.update({path: [response] for path, response in extra.items()})
        paths = ["/left/same.jpg", "/right/same.jpg", *extra]
        expected_success = {"/left/same.jpg", "/right/same.jpg"}
        request_timeout = 1
    elif name == "empty":
        objects = {"/empty.jpg": payloads["JPEG"]}
        policies = {"/empty.jpg": [Response(empty=True)]}
        paths, expected_success = ["/empty.jpg"], set()
    elif name == "retry":
        objects = {"/retry429.jpg": payloads["JPEG"], "/retry503.jpg": payloads["PNG"]}
        policies = {
            path: [Response(status, "0.05"), Response()]
            for path, status in zip(objects, (429, 503), strict=True)
        }
        paths, expected_success, attempts = list(objects), set(objects), 2
    elif name == "deadline":
        objects = {"/slow.jpg": payloads["JPEG"]}
        policies = {"/slow.jpg": [Response(first_byte_delay=2)]}
        paths, expected_success, process_deadline = ["/slow.jpg"], set(), 0.15
    return {
        "name": name,
        "objects": objects,
        "policies": policies,
        "paths": paths,
        "expected_success": expected_success,
        "attempt_budget": attempts,
        "request_timeout": request_timeout,
        "process_deadline": process_deadline,
    }


def policy_record(plan):
    return {
        "case": plan["name"],
        "policies": {
            path: [asdict(item) for item in sequence] for path, sequence in plan["policies"].items()
        },
        "paths": plan["paths"],
        "expected_success_paths": sorted(plan["expected_success"]),
        "attempt_budget": plan["attempt_budget"],
        "request_timeout": plan["request_timeout"],
        "process_deadline": plan["process_deadline"],
        "purpose": "engineering accounting/recovery; unequal native timeouts are not efficacy evidence",
    }

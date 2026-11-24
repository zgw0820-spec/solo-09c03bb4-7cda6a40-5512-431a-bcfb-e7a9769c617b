#!/usr/bin/env python3
"""End-to-end drill against a running aggregation-link drill service.

Uses only the standard library so it can be run from any machine that can
reach the service, with no extra installs:

    python3 drills/run_drill.py --base-url http://localhost:8000
    python3 drills/run_drill.py --base-url http://localhost:8000 --only success

Scenarios:
  success   切除成员链路 A：对端确认排队报文收齐后，流 A 迁往 B；等待期间
            新包进入缓冲，其他流不受影响；摘链后缓冲包经 B 送达。
  timeout   等待时限过短：摘链超时，不迁流，缓冲包仍走原链路 A；迟到的确认
            不会改变结果。
  buffer    缓冲上限为 1：第二个待迁包被拒收，其他流照发。
  reject2   第二个摘链申请在上一个未完成时被拒（一次只处理一个）。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

FAILURES: list[str] = []


def call(base: str, method: str, path: str, body=None, expect: int | None = None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.status
            payload = json.loads(response.read() or b"null")
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read()
        payload = json.loads(raw) if raw else None
    if expect is not None and status != expect:
        raise AssertionError(f"{method} {path} -> {status}, expected {expect}: {payload}")
    return status, payload


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    if not condition:
        FAILURES.append(label)


def wait_until(fn, timeout: float = 10.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(interval)
    return None


def reset(base: str) -> None:
    _, state = call(base, "POST", "/api/reset", expect=200)
    call(base, "PATCH", "/api/links/A", {"delivery_delay_ms": 300, "ack_delay_ms": 200}, expect=200)
    call(base, "PATCH", "/api/links/B", {"delivery_delay_ms": 150, "ack_delay_ms": 80}, expect=200)
    return state


def received(base: str, flow: str) -> list[dict]:
    _, data = call(base, "GET", f"/api/flows/{flow}/received", expect=200)
    return data["delivery_order"]


def scenario_success(base: str) -> None:
    print("\n== 场景 1：摘链成功 ==")
    call(base, "POST", "/api/flows", {"flow_id": "voice", "link": "A"}, expect=201)
    call(base, "POST", "/api/flows", {"flow_id": "video", "link": "B"}, expect=201)
    for seq in (1, 2):
        call(base, "POST", "/api/flows/voice/packets", {"seq": seq}, expect=201)
    print("  已在 A 上排队 voice#1、voice#2")

    _, rm = call(
        base, "POST", "/api/removals",
        {"member_link": "A", "wait_ms": 5000, "buffer_limit": 5}, expect=202,
    )
    print(f"  摘链申请 {rm['removal_id']}：待确认 {rm['snapshot_packet_count']} 包")

    # While waiting, a new packet for the affected flow is buffered.
    _, buffered = call(base, "POST", "/api/flows/voice/packets", {"seq": 3}, expect=202)
    check("等待期间 voice#3 进入缓冲", buffered["buffered"] is True)
    # Another flow keeps sending normally.
    call(base, "POST", "/api/flows/video/packets", {"seq": 1}, expect=201)
    check("其他流 video#1 正常入队", True)

    _, busy = call(
        base, "POST", "/api/removals",
        {"member_link": "B", "wait_ms": 1000, "buffer_limit": 1}, expect=409,
    )
    check("并发的第二个摘链申请被拒绝", busy.get("removal_id") is not None, busy.get("error", ""))

    detail = wait_until(
        lambda: (lambda d: d if d["status"] == "succeeded" else None)(
            call(base, "GET", f"/api/removals/{rm['removal_id']}", expect=200)[1]
        ),
        timeout=10,
    )
    check("摘链成功（对端确认）", detail is not None)
    if detail:
        check("已迁移流包含 voice", detail["moved_flow_ids"] == ["voice"])
        check("缓冲包转投链路 B", detail["flushed_to"] == "B" and len(detail["flushed_packet_ids"]) == 1)

    order = wait_until(lambda: (lambda o: o if len(o) == 3 else None)(received(base, "voice")), timeout=10)
    check("voice 三个包均已送达", order is not None)
    if order:
        check("voice 按接纳顺序送达 1,2,3", [x["seq"] for x in order] == [1, 2, 3])
        check("voice#2 走 A，#3 走 B", [x["link"] for x in order] == ["A", "A", "B"])

    _, flow = call(base, "GET", "/api/flows/voice", expect=200)
    check("voice 当前承载链路为 B", flow["link"] == "B")
    _, vorder = call(base, "GET", "/api/flows/video/received", expect=200)
    check("video 未受摘链影响（仍走 B）", [x["link"] for x in vorder["delivery_order"]] == ["B"])


def scenario_timeout(base: str) -> None:
    print("\n== 场景 2：等待超时，不摘链 ==")
    call(base, "POST", "/api/flows", {"flow_id": "telemetry", "link": "A"}, expect=201)
    call(base, "POST", "/api/flows/telemetry/packets", {"seq": 1}, expect=201)

    _, rm = call(
        base, "POST", "/api/removals",
        {"member_link": "A", "wait_ms": 50, "buffer_limit": 4}, expect=202,
    )
    call(base, "POST", "/api/flows/telemetry/packets", {"seq": 2}, expect=202)
    call(base, "POST", "/api/flows/telemetry/packets", {"seq": 3}, expect=202)

    detail = wait_until(
        lambda: (lambda d: d if d["status"] == "timed_out" else None)(
            call(base, "GET", f"/api/removals/{rm['removal_id']}", expect=200)[1]
        ),
        timeout=10,
    )
    check("摘链超时", detail is not None)
    if detail:
        check("超时后未迁移任何流", detail["moved_flow_ids"] == [])
        check("缓冲的两个包回到原链路 A", detail["flushed_to"] == "A" and len(detail["flushed_packet_ids"]) == 2)

    order = wait_until(lambda: (lambda o: o if len(o) == 3 else None)(received(base, "telemetry")), timeout=10)
    check("三个包最终全部送达", order is not None)
    if order:
        check("全部走原链路 A", [x["link"] for x in order] == ["A", "A", "A"])
        check("送达顺序 1,2,3", [x["seq"] for x in order] == [1, 2, 3])

    detail = call(base, "GET", f"/api/removals/{rm['removal_id']}", expect=200)[1]
    check("迟到的确认被记录且结果仍为超时", detail["status"] == "timed_out" and detail["late_confirmations_ignored"] >= 1)
    _, flow = call(base, "GET", "/api/flows/telemetry", expect=200)
    check("telemetry 仍承载在 A", flow["link"] == "A")


def scenario_buffer(base: str) -> None:
    print("\n== 场景 3：缓冲满则拒收，其他流照发 ==")
    call(base, "POST", "/api/flows", {"flow_id": "sig", "link": "A"}, expect=201)
    call(base, "POST", "/api/flows", {"flow_id": "bulk", "link": "B"}, expect=201)
    call(base, "POST", "/api/flows/sig/packets", {"seq": 1}, expect=201)

    _, rm = call(
        base, "POST", "/api/removals",
        {"member_link": "A", "wait_ms": 5000, "buffer_limit": 1}, expect=202,
    )
    call(base, "POST", "/api/flows/sig/packets", {"seq": 2}, expect=202)
    _, rejected = call(base, "POST", "/api/flows/sig/packets", {"seq": 3}, expect=429)
    check("缓冲已满时 sig#3 被拒收", rejected.get("buffer_limit") == 1)
    call(base, "POST", "/api/flows/bulk/packets", {"seq": 1}, expect=201)
    check("其他流 bulk#1 不受缓冲上限影响", True)

    wait_until(
        lambda: call(base, "GET", f"/api/removals/{rm['removal_id']}", expect=200)[1]["status"] != "pending",
        timeout=10,
    )
    detail = call(base, "GET", f"/api/removals/{rm['removal_id']}", expect=200)[1]
    check("本次摘链成功", detail["status"] == "succeeded")
    check("拒收计数为 1", detail["rejected_count"] == 1)
    # Rejected seq was never accepted, so it can be sent after the removal.
    call(base, "POST", "/api/flows/sig/packets", {"seq": 3}, expect=201)
    order = wait_until(lambda: (lambda o: o if len(o) == 3 else None)(received(base, "sig")), timeout=10)
    check("重发 sig#3 后按 1,2,3 送达", order is not None and [x["seq"] for x in order] == [1, 2, 3])


SCENARIOS = {
    "success": scenario_success,
    "timeout": scenario_timeout,
    "buffer": scenario_buffer,
}


def scenario_after_removal(base: str) -> None:
    print("\n== 场景 4：摘链后成员链路已出组（reset 才能复原）==")
    call(base, "POST", "/api/flows", {"flow_id": "last", "link": "A"}, expect=201)
    _, rm = call(
        base, "POST", "/api/removals",
        {"member_link": "A", "wait_ms": 5000, "buffer_limit": 5}, expect=200,
    )
    check("无待确认报文时立即完成", rm["status"] == "succeeded")
    _, links = call(base, "GET", "/api/links", expect=200)
    by_id = {link["id"]: link for link in links["links"]}
    check("链路 A 标记为已摘除", by_id["A"]["removed"] is True)
    _, denied = call(base, "POST", "/api/flows", {"flow_id": "nope", "link": "A"}, expect=409)
    check("不能再在已摘除的链路上建流", denied.get("link") == "A")
    _, denied = call(
        base, "POST", "/api/removals",
        {"member_link": "B", "wait_ms": 1000, "buffer_limit": 1}, expect=409,
    )
    check("摘除最后一条可用链路被拒绝", "target_link" in denied)


SCENARIOS["after_removal"] = scenario_after_removal


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--only", action="append", choices=sorted(SCENARIOS),
        help="run only the named scenario(s); default: all",
    )
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    try:
        call(base, "GET", "/healthz", expect=200)
    except Exception as exc:  # noqa: BLE001
        print(f"cannot reach {base}: {exc}", file=sys.stderr)
        return 2

    names = args.only or list(SCENARIOS)
    for name in names:
        reset(base)
        SCENARIOS[name](base)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        return 1
    print(f"all checks passed ({len(names)} scenario(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main())

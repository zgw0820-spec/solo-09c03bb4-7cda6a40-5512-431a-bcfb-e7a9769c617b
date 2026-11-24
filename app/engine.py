from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

LINK_IDS = ("A", "B")


def utcnow() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class DrillError(Exception):
    """Domain error with an HTTP status and optional structured context."""

    def __init__(self, status_code: int, message: str, context: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.context = context or {}


@dataclass
class LinkState:
    id: str
    delivery_delay_ms: int
    ack_delay_ms: int
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    worker: asyncio.Task | None = None
    enqueued: int = 0
    delivered: int = 0
    acked: int = 0
    current_packet_id: str | None = None
    current_phase: str = "idle"
    removed: bool = False

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "delivery_delay_ms": self.delivery_delay_ms,
            "ack_delay_ms": self.ack_delay_ms,
            "queue_depth": self.queue.qsize(),
            "enqueued": self.enqueued,
            "delivered": self.delivered,
            "acked": self.acked,
            "current_packet_id": self.current_packet_id,
            "current_phase": self.current_phase,
            "removed": self.removed,
        }


@dataclass
class Flow:
    id: str
    link: str
    created_at: str
    accepted_count: int = 0
    seen_seqs: set[int] = field(default_factory=set)
    received: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "link": self.link,
            "created_at": self.created_at,
            "accepted_count": self.accepted_count,
            "received_count": len(self.received),
        }


@dataclass
class Packet:
    id: str
    flow_id: str
    seq: int
    payload: Any
    accepted_at: str
    accept_index: int
    status: str = "queued"  # queued | delivering | delivered | acked | buffered
    link: str | None = None
    delivered_at: str | None = None
    acked_at: str | None = None
    flow_delivery_index: int | None = None
    buffered_for_removal: str | None = None

    def to_dict(self) -> dict:
        return {
            "packet_id": self.id,
            "flow_id": self.flow_id,
            "seq": self.seq,
            "status": self.status,
            "link": self.link,
            "accepted_at": self.accepted_at,
            "delivered_at": self.delivered_at,
            "acked_at": self.acked_at,
            "flow_delivery_index": self.flow_delivery_index,
            "buffered_for_removal": self.buffered_for_removal,
            "payload": self.payload,
        }


@dataclass
class RemovalRequest:
    id: str
    member_link: str
    target_link: str
    wait_ms: int
    buffer_limit: int
    created_at: str
    status: str = "pending"  # pending | succeeded | timed_out
    finished_at: str | None = None
    result: str | None = None
    reason: str | None = None
    affected_flow_ids: list[str] = field(default_factory=list)
    snapshot_packet_ids: list[str] = field(default_factory=list)
    snapshot_acked: int = 0
    buffered_counts: dict[str, int] = field(default_factory=dict)
    buffered_packet_ids: list[str] = field(default_factory=list)
    flushed_packet_ids: list[str] = field(default_factory=list)
    flushed_to: str | None = None
    rejected_count: int = 0
    late_confirmations_ignored: int = 0
    late_confirmation_packet_ids: list[str] = field(default_factory=list)
    moved_flow_ids: list[str] = field(default_factory=list)
    timeout_task: asyncio.Task | None = field(default=None, repr=False)
    events: list[dict] = field(default_factory=list)

    def add_event(self, event_type: str, **detail: Any) -> None:
        self.events.append({"at": utcnow(), "type": event_type, **detail})

    def to_dict(self) -> dict:
        return {
            "removal_id": self.id,
            "status": self.status,
            "result": self.result,
            "reason": self.reason,
            "member_link": self.member_link,
            "target_link": self.target_link,
            "wait_ms": self.wait_ms,
            "buffer_limit": self.buffer_limit,
            "buffer_used": len(self.buffered_packet_ids),
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "affected_flow_ids": self.affected_flow_ids,
            "moved_flow_ids": self.moved_flow_ids,
            "snapshot_packet_ids": self.snapshot_packet_ids,
            "snapshot_packet_count": len(self.snapshot_packet_ids),
            "snapshot_acked": self.snapshot_acked,
            "buffered_packet_ids": self.buffered_packet_ids,
            "buffered_counts": self.buffered_counts,
            "flushed_packet_ids": self.flushed_packet_ids,
            "flushed_to": self.flushed_to,
            "rejected_count": self.rejected_count,
            "late_confirmations_ignored": self.late_confirmations_ignored,
            "late_confirmation_packet_ids": self.late_confirmation_packet_ids,
            "events": self.events,
        }

    def to_summary(self) -> dict:
        data = self.to_dict()
        data.pop("events", None)
        data.pop("late_confirmation_packet_ids", None)
        return data


class DrillEngine:
    """In-memory drill for removing one member link from a two-member bundle.

    Links A and B are strict FIFO pipes. Each packet is delivered after the
    link's delivery delay and acknowledged after the link's ack delay; a link
    processes one packet at a time. The peer's "catch-up confirmation" for a
    removal is derived from the ACK of every packet that was already queued on
    the member link when the removal request was created.
    """

    def __init__(
        self,
        link_a_delivery_ms: int = 300,
        link_a_ack_ms: int = 200,
        link_b_delivery_ms: int = 150,
        link_b_ack_ms: int = 80,
    ):
        self._lock = asyncio.Lock()
        self._defaults = {
            "A": (link_a_delivery_ms, link_a_ack_ms),
            "B": (link_b_delivery_ms, link_b_ack_ms),
        }
        self.links: dict[str, LinkState] = {}
        self.flows: dict[str, Flow] = {}
        self.removals: dict[str, RemovalRequest] = {}
        self._removal_order: list[str] = []
        self.active_removal_id: str | None = None
        self._packets: dict[str, Packet] = {}
        self.events: list[dict] = []
        self._reset_state()

    # ------------------------------------------------------------------ setup
    def _reset_state(self) -> None:
        self.links = {
            link_id: LinkState(link_id, *self._defaults[link_id]) for link_id in LINK_IDS
        }
        self.flows = {}
        self.removals = {}
        self._removal_order = []
        self.active_removal_id = None
        self._packets = {}
        self.events = []

    async def start(self) -> None:
        for link in self.links.values():
            if link.worker is None or link.worker.done():
                link.worker = asyncio.create_task(
                    self._link_worker(link), name=f"link-{link.id}"
                )

    async def stop(self) -> None:
        tasks: list[asyncio.Task] = []
        for link in self.links.values():
            if link.worker is not None:
                link.worker.cancel()
                tasks.append(link.worker)
                link.worker = None
        for removal in self.removals.values():
            if removal.timeout_task is not None:
                removal.timeout_task.cancel()
                tasks.append(removal.timeout_task)
                removal.timeout_task = None
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    def _log(self, event_type: str, **detail: Any) -> None:
        self.events.append({"at": utcnow(), "type": event_type, **detail})
        if len(self.events) > 2000:
            del self.events[: len(self.events) - 2000]

    # ------------------------------------------------------------------ links
    async def _link_worker(self, link: LinkState) -> None:
        while True:
            packet = await link.queue.get()
            async with self._lock:
                link.current_packet_id = packet.id
                link.current_phase = "delivering"
                packet.status = "delivering"
                delivery_delay = link.delivery_delay_ms
            await asyncio.sleep(delivery_delay / 1000)

            async with self._lock:
                flow = self.flows.get(packet.flow_id)
                if flow is None:
                    link.current_packet_id = None
                    link.current_phase = "idle"
                    continue
                packet.status = "delivered"
                packet.delivered_at = utcnow()
                flow.received.append(
                    {
                        "delivery_index": len(flow.received) + 1,
                        "packet_id": packet.id,
                        "seq": packet.seq,
                        "link": packet.link,
                        "received_at": packet.delivered_at,
                    }
                )
                packet.flow_delivery_index = len(flow.received)
                link.delivered += 1
                link.current_phase = "awaiting_ack"
                ack_delay = link.ack_delay_ms
            await asyncio.sleep(ack_delay / 1000)

            async with self._lock:
                packet.status = "acked"
                packet.acked_at = utcnow()
                link.acked += 1
                link.current_packet_id = None
                link.current_phase = "idle"
                self._on_packet_acked(packet)

    async def update_link(
        self,
        link_id: str,
        delivery_delay_ms: int | None = None,
        ack_delay_ms: int | None = None,
    ) -> dict:
        if link_id not in self.links:
            raise DrillError(404, f"unknown link {link_id!r}", {"links": list(LINK_IDS)})
        async with self._lock:
            link = self.links[link_id]
            if delivery_delay_ms is not None:
                link.delivery_delay_ms = delivery_delay_ms
            if ack_delay_ms is not None:
                link.ack_delay_ms = ack_delay_ms
            self._log(
                "link_updated",
                link=link_id,
                delivery_delay_ms=link.delivery_delay_ms,
                ack_delay_ms=link.ack_delay_ms,
            )
            return link.to_dict()

    # ------------------------------------------------------------------ flows
    async def create_flow(self, flow_id: str | None, link: str) -> dict:
        if link not in LINK_IDS:
            raise DrillError(400, f"link must be one of {LINK_IDS}")
        async with self._lock:
            active = self._active_removal()
            if self.links[link].removed:
                raise DrillError(
                    409,
                    f"link {link} has been removed from the bundle; reset the drill to start over",
                    {"link": link},
                )
            if active is not None and link == active.member_link:
                raise DrillError(
                    409,
                    "cannot create a flow on the member link while it is being removed",
                    {"removal_id": active.id},
                )
            fid = flow_id or f"flow-{uuid.uuid4().hex[:8]}"
            if fid in self.flows:
                raise DrillError(409, f"flow {fid!r} already exists")
            if not isinstance(fid, str) or not fid.strip():
                raise DrillError(400, "flow_id must be a non-empty string")
            flow = Flow(id=fid, link=link, created_at=utcnow())
            self.flows[fid] = flow
            self._log("flow_created", flow_id=fid, link=link)
            return flow.to_dict()

    def _get_flow(self, flow_id: str) -> Flow:
        flow = self.flows.get(flow_id)
        if flow is None:
            raise DrillError(404, f"unknown flow {flow_id!r}")
        return flow

    # ---------------------------------------------------------------- sending
    async def send_packet(self, flow_id: str, seq: int, payload: Any = None) -> dict:
        if seq <= 0:
            raise DrillError(400, "seq must be a positive integer")
        async with self._lock:
            flow = self._get_flow(flow_id)
            if seq in flow.seen_seqs:
                raise DrillError(
                    409,
                    "duplicate seq for this flow; the original packet was kept and this copy was not accepted",
                    {"flow_id": flow_id, "seq": seq},
                )

            active = self._active_removal()
            buffering = (
                active is not None
                and active.status == "pending"
                and flow_id in active.affected_flow_ids
            )

            packet = Packet(
                id=f"pkt-{uuid.uuid4().hex[:12]}",
                flow_id=flow_id,
                seq=seq,
                payload=payload,
                accepted_at=utcnow(),
                accept_index=flow.accepted_count + 1,
            )

            if buffering:
                assert active is not None
                if len(active.buffered_packet_ids) >= active.buffer_limit:
                    active.rejected_count += 1
                    active.add_event(
                        "packet_rejected",
                        flow_id=flow_id,
                        seq=seq,
                        reason="buffer_full",
                        buffered=len(active.buffered_packet_ids),
                        buffer_limit=active.buffer_limit,
                    )
                    raise DrillError(
                        429,
                        "buffer full for the pending link removal; packet rejected",
                        {
                            "removal_id": active.id,
                            "flow_id": flow_id,
                            "seq": seq,
                            "buffer_limit": active.buffer_limit,
                            "buffered_count": len(active.buffered_packet_ids),
                        },
                    )
                packet.status = "buffered"
                packet.buffered_for_removal = active.id
                active.buffered_packet_ids.append(packet.id)
                active.buffered_counts[flow_id] = (
                    active.buffered_counts.get(flow_id, 0) + 1
                )
                active.add_event(
                    "packet_buffered",
                    packet_id=packet.id,
                    flow_id=flow_id,
                    seq=seq,
                    buffered=len(active.buffered_packet_ids),
                )
            else:
                packet.status = "queued"
                packet.link = flow.link
                self._enqueue(packet, flow.link)

            flow.seen_seqs.add(seq)
            flow.accepted_count += 1
            self._packets[packet.id] = packet
            self._log(
                "packet_accepted",
                packet_id=packet.id,
                flow_id=flow_id,
                seq=seq,
                link=packet.link,
                state=packet.status,
            )
            return {
                "accepted": True,
                "buffered": packet.status == "buffered",
                "removal_id": active.id if buffering else None,
                "packet": packet.to_dict(),
                "flow": flow.to_dict(),
            }

    def _enqueue(self, packet: Packet, link_id: str) -> None:
        link = self.links[link_id]
        packet.link = link_id
        packet.status = "queued"
        link.enqueued += 1
        link.queue.put_nowait(packet)

    # ------------------------------------------------------------ peer / views
    async def received_for_flow(self, flow_id: str) -> dict:
        async with self._lock:
            flow = self._get_flow(flow_id)
            return {
                "flow_id": flow_id,
                "link": flow.link,
                "created_at": flow.created_at,
                "accepted_count": flow.accepted_count,
                "delivery_order": list(flow.received),
                "count": len(flow.received),
            }

    async def packet_detail(self, packet_id: str) -> dict:
        async with self._lock:
            packet = self._packets.get(packet_id)
            if packet is None:
                raise DrillError(404, f"unknown packet {packet_id!r}")
            return packet.to_dict()

    # --------------------------------------------------------------- removals
    def _active_removal(self) -> RemovalRequest | None:
        if self.active_removal_id is None:
            return None
        return self.removals.get(self.active_removal_id)

    async def create_removal(
        self, member_link: str, wait_ms: int, buffer_limit: int
    ) -> dict:
        if member_link not in LINK_IDS:
            raise DrillError(400, f"member_link must be one of {LINK_IDS}")
        target_link = "B" if member_link == "A" else "A"
        if wait_ms < 0:
            raise DrillError(400, "wait_ms must be >= 0")
        if buffer_limit < 0:
            raise DrillError(400, "buffer_limit must be >= 0")

        async with self._lock:
            active = self._active_removal()
            if active is not None and active.status == "pending":
                raise DrillError(
                    409,
                    "another link removal is already pending; only one at a time",
                    {"removal_id": active.id},
                )
            if self.links[member_link].removed:
                raise DrillError(
                    409,
                    f"link {member_link} is no longer part of the bundle",
                    {"link": member_link},
                )
            target = self.links[target_link]
            if target.removed:
                raise DrillError(
                    409,
                    f"target link {target_link} has been removed; nothing to move onto",
                    {"target_link": target_link},
                )

            removal = RemovalRequest(
                id=f"rm-{uuid.uuid4().hex[:10]}",
                member_link=member_link,
                target_link=target_link,
                wait_ms=wait_ms,
                buffer_limit=buffer_limit,
                created_at=utcnow(),
            )
            affected = [f for f in self.flows.values() if f.link == member_link]
            removal.affected_flow_ids = [f.id for f in affected]
            # Everything accepted on the member link before this request must be
            # acknowledged by the peer before the flows may move.
            snapshot = [
                p
                for p in self._packets.values()
                if p.flow_id in removal.affected_flow_ids
                and p.status in {"queued", "delivering", "delivered"}
                and p.buffered_for_removal is None
            ]
            snapshot.sort(key=lambda p: (p.flow_id, p.accept_index))
            removal.snapshot_packet_ids = [p.id for p in snapshot]
            removal.snapshot_acked = 0
            self.removals[removal.id] = removal
            self._removal_order.append(removal.id)
            self.active_removal_id = removal.id
            removal.add_event(
                "removal_created",
                member_link=member_link,
                target_link=target_link,
                wait_ms=wait_ms,
                buffer_limit=buffer_limit,
                affected_flow_ids=list(removal.affected_flow_ids),
                snapshot_packet_ids=list(removal.snapshot_packet_ids),
            )
            self._log(
                "removal_created",
                removal_id=removal.id,
                member_link=member_link,
                snapshot_count=len(snapshot),
            )

            if all(self._packet_acked(pid) for pid in removal.snapshot_packet_ids):
                self._finish_removal(removal, "succeeded", "peer_confirmed")
            else:
                removal.timeout_task = asyncio.create_task(
                    self._removal_timeout(removal.id), name=f"timeout-{removal.id}"
                )
            return removal.to_dict()

    def _packet_acked(self, packet_id: str) -> bool:
        packet = self._packets.get(packet_id)
        return packet is not None and packet.status == "acked"

    def _on_packet_acked(self, packet: Packet) -> None:
        for removal in self.removals.values():
            if packet.id not in removal.snapshot_packet_ids:
                continue
            if removal.status == "pending":
                removal.snapshot_acked += 1
                if all(self._packet_acked(pid) for pid in removal.snapshot_packet_ids):
                    self._finish_removal(removal, "succeeded", "peer_confirmed")
            elif removal.status == "timed_out":
                removal.late_confirmations_ignored += 1
                removal.late_confirmation_packet_ids.append(packet.id)
                removal.add_event(
                    "late_confirmation_ignored",
                    packet_id=packet.id,
                    flow_id=packet.flow_id,
                    seq=packet.seq,
                )

    async def _removal_timeout(self, removal_id: str) -> None:
        removal = self.removals[removal_id]
        await asyncio.sleep(removal.wait_ms / 1000)
        async with self._lock:
            if removal.status != "pending":
                return
            self._finish_removal(removal, "timed_out", "wait_timeout")

    def _finish_removal(
        self, removal: RemovalRequest, status: str, reason: str
    ) -> None:
        """Finalize a removal. Must be called while holding the engine lock."""
        if removal.status != "pending":
            return
        removal.status = status
        removal.result = status
        removal.reason = reason
        removal.finished_at = utcnow()
        timeout_task = removal.timeout_task
        if (
            timeout_task is not None
            and timeout_task is not asyncio.current_task()
            and not timeout_task.done()
        ):
            timeout_task.cancel()
        removal.timeout_task = None
        if self.active_removal_id == removal.id:
            self.active_removal_id = None

        if status == "succeeded":
            for flow_id in removal.affected_flow_ids:
                flow = self.flows.get(flow_id)
                if flow is None:
                    continue
                flow.link = removal.target_link
                removal.moved_flow_ids.append(flow_id)
            self.links[removal.member_link].removed = True
            flush_link = removal.target_link
        else:
            flush_link = removal.member_link

        flushed: list[Packet] = []
        for packet_id in removal.buffered_packet_ids:
            packet = self._packets.get(packet_id)
            if packet is None or packet.status != "buffered":
                continue
            flushed.append(packet)
        flushed.sort(key=lambda p: (p.flow_id, p.accept_index))
        for packet in flushed:
            packet.buffered_for_removal = None
            self._enqueue(packet, flush_link)
            removal.flushed_packet_ids.append(packet.id)

        removal.buffered_packet_ids = []
        removal.flushed_to = flush_link
        removal.add_event(
            "removal_finished",
            status=status,
            reason=reason,
            moved_flow_ids=list(removal.moved_flow_ids),
            flushed_packet_ids=list(removal.flushed_packet_ids),
            flushed_to=flush_link,
            snapshot_acked=removal.snapshot_acked,
            rejected_count=removal.rejected_count,
        )
        self._log(
            "removal_finished",
            removal_id=removal.id,
            status=status,
            reason=reason,
            moved=len(removal.moved_flow_ids),
            flushed=len(removal.flushed_packet_ids),
            flushed_to=flush_link,
        )

    # -------------------------------------------------------------- snapshots
    async def state(self) -> dict:
        async with self._lock:
            active = self._active_removal()
            return {
                "links": [self.links[lid].to_dict() for lid in LINK_IDS],
                "flows": [flow.to_dict() for flow in self.flows.values()],
                "active_removal": active.to_summary() if active else None,
                "removal_count": len(self.removals),
                "events": list(self.events[-100:]),
            }

    async def list_removals(self) -> list[dict]:
        async with self._lock:
            return [
                self.removals[rid].to_summary()
                for rid in reversed(self._removal_order)
            ]

    async def get_removal(self, removal_id: str) -> dict:
        async with self._lock:
            removal = self.removals.get(removal_id)
            if removal is None:
                raise DrillError(404, f"unknown removal {removal_id!r}")
            return removal.to_dict()

    async def reset(self) -> dict:
        await self.stop()
        async with self._lock:
            self._reset_state()
            self._log("system_reset")
        await self.start()
        async with self._lock:
            return {
                "reset": True,
                "links": [self.links[lid].to_dict() for lid in LINK_IDS],
            }

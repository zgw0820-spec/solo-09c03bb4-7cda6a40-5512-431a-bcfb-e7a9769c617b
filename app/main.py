from __future__ import annotations

import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .config import EngineConfig, config_from_env
from .engine import DrillEngine, DrillError

LinkId = Literal["A", "B"]
FLOW_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


class FlowCreate(BaseModel):
    flow_id: str | None = Field(default=None, max_length=64)
    link: LinkId

    @field_validator("flow_id")
    @classmethod
    def _validate_flow_id(cls, value: str | None) -> str | None:
        if value is not None and not FLOW_ID_RE.fullmatch(value):
            raise ValueError(
                "flow_id must match [A-Za-z0-9_.-]{1,64} so it can be used in URLs"
            )
        return value


class PacketSend(BaseModel):
    seq: int = Field(gt=0, description="Strictly increasing sequence number per flow")
    payload: Any = None


class LinkUpdate(BaseModel):
    delivery_delay_ms: int | None = Field(default=None, ge=0, le=600_000)
    ack_delay_ms: int | None = Field(default=None, ge=0, le=600_000)


class RemovalCreate(BaseModel):
    member_link: LinkId
    wait_ms: int = Field(
        gt=0, le=600_000, description="How long to wait for the peer confirmation"
    )
    buffer_limit: int = Field(
        ge=0,
        le=100_000,
        description="Max packets buffered across affected flows while waiting",
    )


def _error_response(exc: DrillError) -> JSONResponse:
    body = {"error": exc.message, **exc.context}
    return JSONResponse(status_code=exc.status_code, content=body)


def create_app(engine_config: EngineConfig | None = None) -> FastAPI:
    config = engine_config or config_from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = DrillEngine(
            link_a_delivery_ms=config.link_a.delivery_delay_ms,
            link_a_ack_ms=config.link_a.ack_delay_ms,
            link_b_delivery_ms=config.link_b.delivery_delay_ms,
            link_b_ack_ms=config.link_b.ack_delay_ms,
        )
        app.state.engine = engine
        await engine.start()
        try:
            yield
        finally:
            await engine.stop()

    app = FastAPI(
        title="Aggregation-link removal drill",
        description=(
            "Local FastAPI drill for removing one member link from a two-member "
            "aggregation bundle. Links A and B are simulated FIFO pipes with "
            "adjustable delivery and acknowledgement delays."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(DrillError)
    async def _drill_error_handler(_: Request, exc: DrillError):
        return _error_response(exc)

    def engine(request: Request) -> DrillEngine:
        return request.app.state.engine

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/api/links")
    async def get_links(request: Request):
        state = await engine(request).state()
        return {"links": state["links"]}

    @app.patch("/api/links/{link_id}")
    async def patch_link(link_id: str, body: LinkUpdate, request: Request):
        return await engine(request).update_link(
            link_id.upper(),
            delivery_delay_ms=body.delivery_delay_ms,
            ack_delay_ms=body.ack_delay_ms,
        )

    @app.get("/api/flows")
    async def list_flows(request: Request):
        state = await engine(request).state()
        return {"flows": state["flows"]}

    @app.post("/api/flows", status_code=201)
    async def create_flow(body: FlowCreate, request: Request):
        return await engine(request).create_flow(body.flow_id, body.link)

    @app.post("/api/flows/{flow_id}/packets")
    async def send_packet(
        flow_id: str, body: PacketSend, request: Request, response: Response
    ):
        result = await engine(request).send_packet(flow_id, body.seq, body.payload)
        response.status_code = 202 if result["buffered"] else 201
        return result

    @app.get("/api/flows/{flow_id}")
    async def get_flow(flow_id: str, request: Request):
        state = await engine(request).received_for_flow(flow_id)
        return state

    @app.get("/api/flows/{flow_id}/received")
    async def get_flow_received(flow_id: str, request: Request):
        return await engine(request).received_for_flow(flow_id)

    @app.get("/api/packets/{packet_id}")
    async def get_packet(packet_id: str, request: Request):
        return await engine(request).packet_detail(packet_id)

    @app.post("/api/removals")
    async def create_removal(body: RemovalCreate, request: Request, response: Response):
        result = await engine(request).create_removal(
            body.member_link, body.wait_ms, body.buffer_limit
        )
        response.status_code = 202 if result["status"] == "pending" else 200
        return result

    @app.get("/api/removals")
    async def list_removals(request: Request):
        return {"removals": await engine(request).list_removals()}

    @app.get("/api/removals/{removal_id}")
    async def get_removal(removal_id: str, request: Request):
        return await engine(request).get_removal(removal_id)

    @app.get("/api/state")
    async def get_state(request: Request):
        return await engine(request).state()

    @app.post("/api/reset")
    async def reset(request: Request):
        return await engine(request).reset()

    app.mount(
        "/",
        StaticFiles(directory=Path(__file__).parent / "static", html=True),
        name="static",
    )
    return app


app = create_app()

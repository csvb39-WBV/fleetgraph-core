from __future__ import annotations

from fastapi import FastAPI, Request

from fleetgraph_core.api.batch_endpoint_adapter import apply_batch_endpoint_request
from fleetgraph_core.api.health_version_surface import get_health_status, get_version_info
from fleetgraph_core.api.single_record_endpoint import handle_single_record_request


app = FastAPI()


@app.post("/v1/apply")
async def apply_single_record(request: Request) -> dict[str, object]:
    return handle_single_record_request(await request.json())


@app.post("/v1/apply/batch")
async def apply_batch(request: Request) -> dict[str, object]:
    return apply_batch_endpoint_request(await request.json())


@app.get("/v1/health")
def get_health() -> dict[str, object]:
    return get_health_status()


@app.get("/v1/version")
def get_version() -> dict[str, object]:
    return get_version_info()

from __future__ import annotations

import uvicorn

from fleetgraph_core.runtime.runtime_http_api import app


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000


def launch_runtime_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    uvicorn.run(app, host=host, port=port)


def main() -> None:
    launch_runtime_server()


if __name__ == "__main__":
    main()

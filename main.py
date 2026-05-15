from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable


async def _serve_api(host: str, port: int) -> None:
    import uvicorn

    config = uvicorn.Config(
        "flowforge.api.app:app",
        host=host,
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def _run_worker() -> None:
    from flowforge.worker.worker import main as worker_main

    await worker_main()


async def _run_starter() -> None:
    from flowforge.api.starter import main as starter_main

    await starter_main()


async def _run_all(host: str, port: int) -> None:
    tasks = [
        asyncio.create_task(_serve_api(host, port), name="api"),
        asyncio.create_task(_run_worker(), name="worker"),
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    for task in pending:
        task.cancel()
    for task in done:
        task.result()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FlowForge service entrypoint")
    parser.add_argument(
        "service",
        choices=("api", "worker", "starter", "all"),
        nargs="?",
        default="all",
        help="service to start",
    )
    parser.add_argument("--host", default="0.0.0.0", help="API bind host")
    parser.add_argument("--port", type=int, default=8000, help="API bind port")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    runners: dict[str, Callable[[], Awaitable[None]]] = {
        "api": lambda: _serve_api(args.host, args.port),
        "worker": _run_worker,
        "starter": _run_starter,
        "all": lambda: _run_all(args.host, args.port),
    }
    asyncio.run(runners[args.service]())


if __name__ == "__main__":
    main()

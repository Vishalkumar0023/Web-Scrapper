#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def _api_base_url(host: str, port: int) -> str:
    normalized = host.strip()
    if normalized in {"0.0.0.0", "::", ""}:
        normalized = "127.0.0.1"
    return f"http://{normalized}:{port}"


def _terminate_processes(processes: list[subprocess.Popen[object]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()

    deadline = time.time() + 3
    while time.time() < deadline:
        if all(process.poll() is not None for process in processes):
            return
        time.sleep(0.1)

    for process in processes:
        if process.poll() is None:
            process.kill()


def _run_fullstack(
    *,
    api_cmd: list[str],
    api_dir: Path,
    web_dir: Path,
    api_base_url: str,
    web_port: int,
) -> int:
    package_manager: str | None = None
    web_cmd: list[str] = []
    if shutil.which("pnpm") is not None:
        package_manager = "pnpm"
        web_cmd = ["pnpm", "exec", "next", "dev", "-p", str(web_port)]
    elif shutil.which("npm") is not None:
        package_manager = "npm"
        web_cmd = ["npm", "run", "dev"]
    else:
        print("pnpm or npm is required for --with-web. Install one and run again.", file=sys.stderr)
        return 2

    web_env = os.environ.copy()
    web_env["NEXT_PUBLIC_API_BASE_URL"] = api_base_url

    api_process = subprocess.Popen(api_cmd, cwd=str(api_dir))
    web_process = subprocess.Popen(web_cmd, cwd=str(web_dir), env=web_env)
    processes = [api_process, web_process]

    print(f"[app.py] API: {api_base_url}")
    print(f"[app.py] Web: http://127.0.0.1:{web_port}")
    print(f"[app.py] Frontend runner: {package_manager}")

    try:
        while True:
            api_code = api_process.poll()
            web_code = web_process.poll()

            if api_code is None and web_code is None:
                time.sleep(0.2)
                continue

            _terminate_processes(processes)
            if api_code not in {None, 0}:
                return int(api_code)
            if web_code not in {None, 0}:
                return int(web_code)
            return 0
    except KeyboardInterrupt:
        _terminate_processes(processes)
        return 0


def _clean_web_cache(web_dir: Path) -> None:
    next_dir = web_dir / ".next"
    if not next_dir.exists():
        return

    backup_dir = web_dir / f".next.stale.{int(time.time())}"
    next_dir.rename(backup_dir)
    print(f"[app.py] Moved stale frontend cache: {backup_dir}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run WebScrapper API or full-stack dev server.")
    parser.add_argument("--host", default=os.getenv("API_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("API_PORT", "8000")))
    parser.add_argument("--web-port", type=int, default=int(os.getenv("WEB_PORT", "3000")))
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--with-web", action="store_true", help="Run API and frontend together")
    parser.add_argument(
        "--clean-web-cache",
        action="store_true",
        help="Move apps/web/.next aside before starting frontend (fixes stale chunk errors)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    api_dir = repo_root / "apps" / "api"
    web_dir = repo_root / "apps" / "web"
    if not api_dir.exists():
        print(f"API directory not found: {api_dir}", file=sys.stderr)
        return 2
    if args.with_web and not web_dir.exists():
        print(f"Web directory not found: {web_dir}", file=sys.stderr)
        return 2

    api_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]
    if args.reload:
        api_cmd.append("--reload")

    api_base = _api_base_url(args.host, args.port)
    if args.with_web:
        if args.clean_web_cache:
            _clean_web_cache(web_dir)
        return _run_fullstack(
            api_cmd=api_cmd,
            api_dir=api_dir,
            web_dir=web_dir,
            api_base_url=api_base,
            web_port=args.web_port,
        )

    try:
        return subprocess.call(api_cmd, cwd=str(api_dir))
    except FileNotFoundError as exc:
        print(f"Failed to start API: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

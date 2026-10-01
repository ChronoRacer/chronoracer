#!/usr/bin/env python3
"""Interactive Relay operator HTTP client.

Environment:
  RELAY_BASE_URL     Relay origin (default: https://relay.chronoracer.live)
  OKTA_ACCESS_TOKEN  Bearer token for operator calls (see docs/component-okta.md)
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx
import questionary
from questionary import Style
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

DEFAULT_BASE_URL = "https://relay.chronoracer.live"
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_START_BODY = SCRIPT_DIR / "race_start_request.json"

MENU_STYLE = Style(
    [
        ("qmark", "fg:#673ab7 bold"),
        ("question", "bold"),
        ("answer", "fg:#673ab7 bold"),
        ("pointer", "fg:#673ab7 bold"),
        ("highlighted", "fg:#673ab7 bold"),
        ("selected", "fg:#cc5454"),
    ]
)

console = Console()


def env_config() -> tuple[str, str]:
    base = os.environ.get("RELAY_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    token = os.environ.get("OKTA_ACCESS_TOKEN", "").strip()
    if not token:
        console.print(
            "[red]OKTA_ACCESS_TOKEN is not set.[/red] "
            "Mint a token per docs/component-okta.md (Operator token)."
        )
        sys.exit(1)
    return base, token


def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def print_response(response: httpx.Response) -> None:
    status_color = "green" if response.is_success else "red"
    console.print(f"\n[{status_color}]HTTP {response.status_code}[/{status_color}] {response.request.method} {response.request.url}")

    if response.status_code == 204:
        console.print("[dim](no body)[/dim]")
        return

    text = response.text
    if not text:
        console.print("[dim](empty body)[/dim]")
        return

    content_type = response.headers.get("content-type", "")
    if "json" in content_type or text.lstrip().startswith(("{", "[")):
        try:
            parsed = response.json()
            console.print(Syntax(json.dumps(parsed, indent=2, sort_keys=True), "json", theme="monokai"))
        except json.JSONDecodeError:
            console.print(Panel(text, title="Response body", border_style="yellow"))
    else:
        console.print(Panel(text, title="Response body", border_style="yellow"))


def run_request(fn) -> None:
    try:
        response = fn()
    except httpx.RequestError as exc:
        console.print(f"[red]Request failed:[/red] {exc}")
        return
    print_response(response)


def load_start_body() -> bytes:
    use_default = questionary.confirm(
        f"Use default start body ({DEFAULT_START_BODY.name})?",
        default=True,
        style=MENU_STYLE,
    ).ask()
    if use_default is False:
        path_str = questionary.path(
            "Path to start JSON file",
            default=str(DEFAULT_START_BODY),
            style=MENU_STYLE,
        ).ask()
        if not path_str:
            console.print("[yellow]Cancelled.[/yellow]")
            return b""
        path = Path(path_str)
    else:
        path = DEFAULT_START_BODY

    if not path.is_file():
        console.print(f"[red]File not found:[/red] {path}")
        return b""
    return path.read_bytes()


def default_race_id() -> str:
    try:
        data = json.loads(DEFAULT_START_BODY.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    value = data.get("raceId")
    return str(value) if value else ""


def prompt_race_id(label: str = "Race ID") -> str | None:
    race_id = questionary.text(label, default=default_race_id(), style=MENU_STYLE).ask()
    if race_id is None:
        return None
    race_id = race_id.strip()
    if not race_id:
        console.print("[yellow]Cancelled.[/yellow]")
        return None
    return race_id


def action_start(client: httpx.Client, token: str) -> None:
    body = load_start_body()
    if not body:
        return

    def do() -> httpx.Response:
        return client.post(
            "/race/start",
            headers={**auth_headers(token), "Content-Type": "application/json"},
            content=body,
        )

    run_request(do)


def action_status(client: httpx.Client, token: str) -> None:
    run_request(lambda: client.get("/race/status", headers=auth_headers(token)))


def action_stop(client: httpx.Client, token: str) -> None:
    if not questionary.confirm("Graceful stop the active race?", default=False, style=MENU_STYLE).ask():
        console.print("[yellow]Cancelled.[/yellow]")
        return
    run_request(lambda: client.post("/race/stop", headers=auth_headers(token)))


def action_force_destroy(client: httpx.Client, token: str) -> None:
    if not questionary.confirm(
        "Force destroy the active race? This cannot be undone.",
        default=False,
        style=MENU_STYLE,
    ).ask():
        console.print("[yellow]Cancelled.[/yellow]")
        return
    run_request(lambda: client.post("/race/force-destroy", headers=auth_headers(token)))


def action_history_list(client: httpx.Client, token: str) -> None:
    run_request(lambda: client.get("/race/history", headers=auth_headers(token)))


def action_history_race(client: httpx.Client, token: str, export: bool) -> None:
    race_id = prompt_race_id()
    if race_id is None:
        return
    params = {"export_race": "true"} if export else None

    def do() -> httpx.Response:
        return client.get(f"/race/history/{race_id}", headers=auth_headers(token), params=params)

    run_request(do)


def action_event(client: httpx.Client, token: str) -> None:
    key = questionary.text("Kafka message key (query param `key`)", style=MENU_STYLE).ask()
    if key is None or not key.strip():
        console.print("[yellow]Cancelled.[/yellow]")
        return
    body = questionary.text("Raw request body", default="opaque-bytes", style=MENU_STYLE).ask()
    if body is None:
        console.print("[yellow]Cancelled.[/yellow]")
        return
    payload = body.encode("utf-8")

    def do() -> httpx.Response:
        return client.post(
            "/race/event",
            params={"key": key.strip()},
            headers=auth_headers(token),
            content=payload,
        )

    run_request(do)


ACTIONS: list[tuple[str, str]] = [
    ("start", "Start race (POST /race/start)"),
    ("status", "Race status (GET /race/status)"),
    ("stop", "Graceful stop (POST /race/stop)"),
    ("force_destroy", "Force destroy (POST /race/force-destroy)"),
    ("history_list", "History list (GET /race/history)"),
    ("history", "Historical race (GET /race/history/{id})"),
    ("history_export", "Historical race + export URLs (export_race=true)"),
    ("event", "Operator event (POST /race/event)"),
]


def dispatch(action_id: str, client: httpx.Client, token: str) -> None:
    if action_id == "start":
        action_start(client, token)
    elif action_id == "status":
        action_status(client, token)
    elif action_id == "stop":
        action_stop(client, token)
    elif action_id == "force_destroy":
        action_force_destroy(client, token)
    elif action_id == "history_list":
        action_history_list(client, token)
    elif action_id == "history":
        action_history_race(client, token, export=False)
    elif action_id == "history_export":
        action_history_race(client, token, export=True)
    elif action_id == "event":
        action_event(client, token)


def print_banner(base_url: str) -> None:
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_row("[bold]Relay[/bold]", base_url)
    table.add_row("[dim]Token[/dim]", "OKTA_ACCESS_TOKEN (set)")
    console.print(Panel(table, title="Relay operator CLI", border_style="blue"))


def main() -> None:
    base_url, token = env_config()
    print_banner(base_url)

    choices = [label for _, label in ACTIONS] + ["Exit"]
    id_by_label = {label: action_id for action_id, label in ACTIONS}

    with httpx.Client(base_url=base_url, timeout=120.0) as client:
        while True:
            choice = questionary.select(
                "Choose an operation (↑↓, Enter)",
                choices=choices,
                style=MENU_STYLE,
            ).ask()
            if choice is None or choice == "Exit":
                break
            action_id = id_by_label[choice]
            dispatch(action_id, client, token)
            questionary.press_any_key_to_continue("Press any key to return to the menu…").ask()


if __name__ == "__main__":
    main()

"""Concurrent TCP server for the Unit 1 IPC exercise."""

from __future__ import annotations

import argparse
import socket
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from protocol import marshal, recv_message
from storage import KeyValueStore, TaskQueue


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9090
DEFAULT_MAX_WORKERS = 10


def process_request(
    message: dict[str, Any],
    store: KeyValueStore,
    task_queue: TaskQueue,
) -> dict[str, Any]:
    """Execute one decoded request and return its response."""
    action = message.get("action")
    if not isinstance(action, str):
        raise ValueError("action must be a string")

    if action == "PUT":
        key = message.get("key")
        if not isinstance(key, str):
            raise ValueError("PUT requires a string key")
        if "value" not in message:
            raise ValueError("PUT requires a value")
        store.put(key, message["value"])
        return {"status": "OK"}

    if action == "GET":
        key = message.get("key")
        if not isinstance(key, str):
            raise ValueError("GET requires a string key")
        if not store.contains(key):
            return {"status": "NOT_FOUND", "key": key}
        return {"status": "OK", "key": key, "value": store.get(key)}

    if action == "ENQUEUE":
        if "item" not in message:
            raise ValueError("ENQUEUE requires an item")
        task_queue.enqueue(message["item"])
        return {"status": "OK"}

    if action == "DEQUEUE":
        timeout = message.get("timeout")
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout < 0
        ):
            raise ValueError(
                "DEQUEUE timeout must be a non-negative number or null")
        try:
            item = task_queue.dequeue(timeout=timeout)
        except TimeoutError:
            return {"status": "EMPTY"}
        return {"status": "OK", "item": item}

    raise ValueError(f"unsupported action: {action}")


def handle_client(
    client_socket: socket.socket,
    store: KeyValueStore,
    task_queue: TaskQueue,
) -> None:
    """Receive one request, execute it, and send one response."""
    with client_socket:
        try:
            message = recv_message(client_socket)
            response = process_request(message, store, task_queue)
        except (EOFError, UnicodeDecodeError, ValueError, TypeError, OSError) as error:
            response = {"status": "ERROR", "error": str(error)}
        client_socket.sendall(marshal(response))


def serve(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
          max_workers: int = DEFAULT_MAX_WORKERS) -> None:
    """Listen for TCP connections and dispatch them to a bounded worker pool."""
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()

        print(f"[*] Server initialized on {host}:{port}", flush=True)
        print(
            f"[*] Thread Pool initialized with max_workers={max_workers}...",
            flush=True,
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            store = KeyValueStore()
            task_queue = TaskQueue()
            while True:
                client_socket, _ = server_socket.accept()
                executor.submit(
                    handle_client,
                    client_socket,
                    store,
                    task_queue,
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the concurrent TCP server.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    serve(arguments.host, arguments.port, arguments.workers)

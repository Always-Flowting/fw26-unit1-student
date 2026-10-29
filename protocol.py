"""Length-prefixed JSON message framing for the TCP server."""

from __future__ import annotations

import json
import socket
import struct
from typing import Any


HEADER_SIZE = 4
MAX_PAYLOAD_SIZE = 1_048_576


def marshal(message: dict[str, Any]) -> bytes:
    """Convert a JSON message into a 4-byte-length-prefixed frame."""
    payload = json.dumps(message, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError("message payload is too large")
    return struct.pack("!I", len(payload)) + payload


def recv_exact(client_socket: socket.socket, size: int) -> bytes:
    """Read exactly `size` bytes, or raise EOFError if the peer disconnects."""
    chunks = bytearray()
    while len(chunks) < size:
        chunk = client_socket.recv(size - len(chunks))
        if not chunk:
            raise EOFError("client disconnected before the complete message arrived")
        chunks.extend(chunk)
    return bytes(chunks)


def unmarshal(frame: bytes) -> dict[str, Any]:
    """Convert one complete length-prefixed frame into a JSON object."""
    if len(frame) < HEADER_SIZE:
        raise ValueError("frame is missing its 4-byte header")

    payload_size = struct.unpack("!I", frame[:HEADER_SIZE])[0]
    if payload_size > MAX_PAYLOAD_SIZE:
        raise ValueError("message payload is too large")
    if len(frame) != HEADER_SIZE + payload_size:
        raise ValueError("frame length does not match its header")

    message = json.loads(frame[HEADER_SIZE:].decode("utf-8"))
    if not isinstance(message, dict):
        raise ValueError("message payload must be a JSON object")
    return message


def recv_message(client_socket: socket.socket) -> dict[str, Any]:
    """Receive and decode one complete framed JSON message."""
    header = recv_exact(client_socket, HEADER_SIZE)
    payload_size = struct.unpack("!I", header)[0]
    if payload_size > MAX_PAYLOAD_SIZE:
        raise ValueError("message payload is too large")
    payload = recv_exact(client_socket, payload_size)
    return unmarshal(header + payload)
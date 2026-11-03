from concurrent.futures import ThreadPoolExecutor
import socket
from typing import Any

from protocol import marshal, recv_message


HOST = "127.0.0.1"
PORT = 9090
REQUEST_COUNT = 20


def send_request(request_number: int) -> None:
    """Send one request over its own TCP connection."""
    request = marshal({
        "action": "PUT",
        "key": f"stress-test-{request_number}",
        "value": request_number,
    })

    with socket.create_connection((HOST, PORT)) as client:
        client.sendall(request)
        print(f"[Request {request_number:02d}] {recv_message(client)}")


def main() -> None:
    """Send a burst of concurrent requests and print every response."""
    print(f"[*] Sending {REQUEST_COUNT} concurrent requests...")
    with ThreadPoolExecutor(max_workers=REQUEST_COUNT) as executor:
        for i in range(REQUEST_COUNT):
            executor.submit(send_request, i)

    print(f"[+] All {REQUEST_COUNT} requests completed successfully.")


if __name__ == "__main__":
    main()

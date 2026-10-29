import socket
from protocol import marshal, recv_message

request = marshal({
    "action": "PUT",
    "key": "username",
    "value": "alice",
})

with socket.create_connection(("127.0.0.1", 9090)) as client:
    client.sendall(request)
    response = recv_message(client)
    print(response)

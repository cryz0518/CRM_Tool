"""将受限的 CRM HTTP 请求通过固定 SOCKS5 上游转发。

该服务只允许访问配置的 SOP Gateway 目标，不接受任意目标，避免本地开发代理
意外变成开放转发器。它只处理当前 CRM Adapter 使用的 HTTP 明文请求。
"""

from __future__ import annotations

import os
import socket
import socketserver
import struct
from dataclasses import dataclass
from urllib.parse import urlsplit

MAX_HEADER_BYTES = 64 * 1024
MAX_BODY_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class ProxyConfig:
    """保存代理监听、SOCKS 上游和唯一 CRM 目标。"""

    listen_host: str
    listen_port: int
    socks_host: str
    socks_port: int
    target_host: str
    target_connect_host: str
    target_port: int

    @classmethod
    def from_env(cls) -> "ProxyConfig":
        """从环境变量加载配置；空值或非法端口直接阻止代理启动。"""

        return cls(
            listen_host=os.environ.get("LISTEN_HOST", "0.0.0.0"),
            listen_port=int(os.environ.get("LISTEN_PORT", "8080")),
            socks_host=os.environ["SOCKS_HOST"],
            socks_port=int(os.environ["SOCKS_PORT"]),
            target_host=os.environ["TARGET_HOST"].lower(),
            target_connect_host=os.environ["TARGET_CONNECT_HOST"],
            target_port=int(os.environ["TARGET_PORT"]),
        )


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    """读取指定字节数；对端提前关闭时抛出连接错误。"""

    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("upstream closed connection")
        data.extend(chunk)
    return bytes(data)


def _socks5_connect(config: ProxyConfig) -> socket.socket:
    """通过无认证 SOCKS5 上游连接固定 CRM 地址。"""

    upstream = socket.create_connection((config.socks_host, config.socks_port), 10)
    upstream.settimeout(20)
    upstream.sendall(b"\x05\x01\x00")
    version, method = _recv_exact(upstream, 2)
    if version != 5 or method != 0:
        upstream.close()
        raise ConnectionError("SOCKS5 authentication is unavailable")

    try:
        connect_ip = socket.inet_aton(config.target_connect_host)
    except OSError:
        target = config.target_connect_host.encode("idna")
        if len(target) > 255:
            upstream.close()
            raise ValueError("SOCKS5 target hostname is too long")
        address = b"\x03" + bytes([len(target)]) + target
    else:
        address = b"\x01" + connect_ip

    upstream.sendall(b"\x05\x01\x00" + address + struct.pack("!H", config.target_port))
    response = _recv_exact(upstream, 4)
    reply_code = response[1]
    address_type = response[3]
    if address_type == 1:
        _recv_exact(upstream, 4)
    elif address_type == 3:
        _recv_exact(upstream, _recv_exact(upstream, 1)[0])
    elif address_type == 4:
        _recv_exact(upstream, 16)
    _recv_exact(upstream, 2)
    if reply_code != 0:
        upstream.close()
        raise ConnectionError("SOCKS5 target connection failed")
    return upstream


def _read_headers(client: socket.socket) -> tuple[bytes, bytes]:
    """读取 HTTP 请求头和已声明的请求体，拒绝过大或分块请求。"""

    raw = bytearray()
    while b"\r\n\r\n" not in raw:
        chunk = client.recv(4096)
        if not chunk:
            raise ConnectionError("client closed before headers")
        raw.extend(chunk)
        if len(raw) > MAX_HEADER_BYTES:
            raise ValueError("request headers are too large")
    head, body = bytes(raw).split(b"\r\n\r\n", 1)
    header_lines = head.split(b"\r\n")
    headers = {
        line.split(b":", 1)[0].decode("latin-1").lower(): line.split(b":", 1)[1].strip()
        for line in header_lines[1:]
        if b":" in line
    }
    if headers.get("transfer-encoding", b"").lower() not in (b"", b"identity"):
        raise ValueError("chunked requests are not supported")
    try:
        body_length = int(headers.get("content-length", b"0"))
    except ValueError as error:
        raise ValueError("invalid content length") from error
    if body_length < 0 or body_length > MAX_BODY_BYTES:
        raise ValueError("request body is too large")
    if len(body) < body_length:
        body += _recv_exact(client, body_length - len(body))
    return head, body[:body_length]


def _request_target(raw_target: str, headers: dict[str, bytes]) -> tuple[str, str]:
    """解析代理绝对 URI 或普通请求路径，返回目标主机和转发路径。"""

    if raw_target.startswith(("http://", "https://")):
        parsed = urlsplit(raw_target)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        return parsed.hostname or "", path
    host = headers.get("host", b"").decode("latin-1").split(":", 1)[0]
    return host, raw_target or "/"


class CRMProxyHandler(socketserver.BaseRequestHandler):
    """处理单个 CRM HTTP 请求并通过 SOCKS5 完成转发。"""

    def handle(self) -> None:
        """校验固定目标、转发请求并回传响应；失败时只返回通用状态。"""

        config: ProxyConfig = self.server.config  # type: ignore[attr-defined]
        upstream: socket.socket | None = None
        try:
            head, body = _read_headers(self.request)
            lines = head.split(b"\r\n")
            method, target, _version = lines[0].decode("latin-1").split(" ", 2)
            header_map = {
                line.split(b":", 1)[0].decode("latin-1").lower(): line.split(b":", 1)[1].strip()
                for line in lines[1:]
                if b":" in line
            }
            host, path = _request_target(target, header_map)
            if host.lower() != config.target_host:
                self._reply(403, b"target denied")
                return

            upstream = _socks5_connect(config)
            forward_headers = []
            for line in lines[1:]:
                key = line.split(b":", 1)[0].lower() if b":" in line else b""
                if key in {b"proxy-connection", b"connection", b"host", b"content-length"}:
                    continue
                forward_headers.append(line)
            forward_headers.extend(
                [
                    b"Host: " + config.target_host.encode("idna"),
                    b"Content-Length: " + str(len(body)).encode("ascii"),
                    b"Connection: close",
                ]
            )
            upstream.sendall(
                (f"{method} {path} HTTP/1.1\r\n").encode("latin-1")
                + b"\r\n".join(forward_headers)
                + b"\r\n\r\n"
                + body
            )
            while True:
                chunk = upstream.recv(64 * 1024)
                if not chunk:
                    break
                self.request.sendall(chunk)
        except TimeoutError:
            self._reply(504, b"upstream timeout")
        except (ConnectionError, OSError, ValueError):
            self._reply(502, b"upstream unavailable")
        finally:
            if upstream is not None:
                upstream.close()

    def _reply(self, status: int, body: bytes) -> None:
        """返回不含上游内容的通用错误，避免泄漏请求或响应数据。"""

        response = (
            f"HTTP/1.1 {status} Proxy Error\r\n"
            "Content-Type: text/plain\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii") + body
        self.request.sendall(response)


class CRMProxyServer(socketserver.ThreadingTCPServer):
    """允许并发处理 CRM 请求的线程 TCP 服务。"""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, config: ProxyConfig) -> None:
        """绑定监听地址并保存固定目标配置。"""

        self.config = config
        super().__init__((config.listen_host, config.listen_port), CRMProxyHandler)


def main() -> None:
    """启动本地 CRM HTTP→SOCKS5 转发服务。"""

    config = ProxyConfig.from_env()
    with CRMProxyServer(config) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()

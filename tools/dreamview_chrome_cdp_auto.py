#!/usr/bin/env python3
"""Enter Apollo Dreamview+ default mode through Chrome DevTools Protocol.

This helper intentionally avoids Apollo/CyberRT imports and browser framework
runtime dependencies. It talks to an already-started Chrome instance that was
launched with ``--remote-debugging-port``.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import struct
import time
from urllib.parse import urlparse
from urllib.request import ProxyHandler, build_opener


def _read_http_json(url: str) -> object:
    opener = build_opener(ProxyHandler({}))
    with opener.open(url, timeout=3.0) as response:  # noqa: S310 - localhost CDP only.
        return json.loads(response.read().decode("utf-8"))


class _CDPWebSocket:
    def __init__(self, ws_url: str) -> None:
        parsed = urlparse(ws_url)
        if parsed.scheme != "ws":
            raise ValueError(f"unsupported CDP websocket URL: {ws_url}")
        self._host = parsed.hostname or "127.0.0.1"
        self._port = int(parsed.port or 80)
        self._path = parsed.path
        if parsed.query:
            self._path += "?" + parsed.query
        self._sock = socket.create_connection((self._host, self._port), timeout=5.0)
        self._next_id = 1
        self._handshake()

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    def _handshake(self) -> None:
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET {self._path} HTTP/1.1\r\n"
            f"Host: {self._host}:{self._port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        self._sock.sendall(request.encode("ascii"))
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self._sock.recv(4096)
            if not chunk:
                break
            data += chunk
        if b" 101 " not in data.split(b"\r\n", 1)[0]:
            raise RuntimeError(f"CDP websocket handshake failed: {data[:200]!r}")
        accept_src = key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        expected = base64.b64encode(hashlib.sha1(accept_src.encode("ascii")).digest())
        if expected not in data:
            raise RuntimeError("CDP websocket handshake accept key mismatch")

    def _send_frame(self, payload: bytes) -> None:
        header = bytearray([0x81])
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.extend([0x80 | 126])
            header.extend(struct.pack("!H", length))
        else:
            header.extend([0x80 | 127])
            header.extend(struct.pack("!Q", length))
        mask = os.urandom(4)
        masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        self._sock.sendall(bytes(header) + mask + masked)

    def _recv_exact(self, count: int) -> bytes:
        data = b""
        while len(data) < count:
            chunk = self._sock.recv(count - len(data))
            if not chunk:
                raise RuntimeError("CDP websocket closed")
            data += chunk
        return data

    def _recv_frame(self) -> bytes:
        first, second = self._recv_exact(2)
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(8))[0]
        mask = self._recv_exact(4) if masked else b""
        payload = self._recv_exact(length)
        if masked:
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        if opcode == 0x8:
            raise RuntimeError("CDP websocket close frame received")
        if opcode == 0x9:
            return self._recv_frame()
        return payload

    def call(self, method: str, params: dict | None = None, timeout_s: float = 5.0) -> dict:
        msg_id = self._next_id
        self._next_id += 1
        payload = {"id": msg_id, "method": method}
        if params is not None:
            payload["params"] = params
        self._send_frame(json.dumps(payload).encode("utf-8"))
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            self._sock.settimeout(max(0.1, deadline - time.time()))
            response = json.loads(self._recv_frame().decode("utf-8"))
            if response.get("id") == msg_id:
                if "error" in response:
                    raise RuntimeError(f"CDP call {method} failed: {response['error']}")
                return response
        raise TimeoutError(f"CDP call timed out: {method}")


AUTO_JS = r"""
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const click = (el) => {
    if (!el) return false;
    el.dispatchEvent(new MouseEvent('mousemove', { bubbles: true }));
    el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
    el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
    el.click();
    return true;
  };
  const clickPoint = (x, y) => {
    const el = document.elementFromPoint(x, y);
    if (!el) return false;
    for (const type of ['mousemove', 'mousedown', 'mouseup', 'click']) {
      el.dispatchEvent(new MouseEvent(type, {
        bubbles: true,
        cancelable: true,
        view: window,
        clientX: x,
        clientY: y,
        button: 0,
      }));
    }
    return true;
  };
  const byButtonText = (needle) => Array.from(document.querySelectorAll('button'))
    .find((el) => (el.innerText || el.textContent || '').match(needle));
  const byAnyText = (needle) => Array.from(document.querySelectorAll('button, a, span, div'))
    .find((el) => (el.innerText || el.textContent || '').match(needle));
  const bodyText = () => document.body ? (document.body.innerText || document.body.textContent || '') : '';

  if (!location.href.startsWith('__URL__')) {
    location.href = '__URL__';
    await sleep(1200);
  }

  const initialText = bodyText();
  const isWelcome = initialText.includes('Welcome,') && initialText.includes('Enter this mode');
  if (isWelcome || initialText.includes('Accept') || initialText.includes('User Agreement')) {
    const checkbox = document.querySelector('input[type="checkbox"]') ||
      document.querySelector('[role="checkbox"]');
    if (checkbox && !checkbox.checked) click(checkbox);
    clickPoint(Math.round(window.innerWidth * 0.665), Math.round(window.innerHeight * 0.875));
    await sleep(250);
    const enter = byButtonText(/Enter this mode/i);
    if (enter && !enter.disabled && !enter.getAttribute('disabled')) click(enter);
    await sleep(250);
    clickPoint(Math.round(window.innerWidth * 0.74), Math.round(window.innerHeight * 0.93));
  }

  await sleep(800);
  const skip = byButtonText(/Skip/i) || byAnyText(/^Skip/i);
  if (skip) click(skip);
  document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', code: 'Escape', bubbles: true }));
  document.dispatchEvent(new KeyboardEvent('keyup', { key: 'Escape', code: 'Escape', bubbles: true }));
  clickPoint(Math.round(window.innerWidth * 0.40), Math.round(window.innerHeight * 0.40));

  await sleep(500);
  const text = bodyText();
  return {
    url: location.href,
    title: document.title,
    ready: text.includes('Mode Settings') && !text.includes('Enter this mode'),
    has_welcome: text.includes('Welcome,') && text.includes('Enter this mode'),
    has_skip: /Skip/.test(text),
    has_baguang: text.includes('Straight Road For Baguang'),
  };
})()
"""


def _target_ws_url(port: int, target_url: str) -> str:
    targets = _read_http_json(f"http://127.0.0.1:{port}/json/list")
    if not isinstance(targets, list):
        raise RuntimeError("unexpected CDP /json/list payload")
    pages = [t for t in targets if isinstance(t, dict) and t.get("type") == "page"]
    for target in pages:
        if str(target.get("url") or "").startswith(target_url):
            return str(target["webSocketDebuggerUrl"])
    if pages:
        return str(pages[0]["webSocketDebuggerUrl"])
    raise RuntimeError("no Chrome CDP page target found")


def auto_enter(url: str, port: int, timeout_s: float) -> dict:
    deadline = time.time() + max(timeout_s, 1.0)
    last: dict = {}
    while time.time() < deadline:
        try:
            ws_url = _target_ws_url(port, url)
        except Exception as exc:
            last = {"target_error": str(exc)}
            time.sleep(1.0)
            continue
        client = _CDPWebSocket(ws_url)
        try:
            client.call("Page.bringToFront", {}, timeout_s=2.0)
            expr = AUTO_JS.replace("__URL__", url.replace("\\", "\\\\").replace("'", "\\'"))
            response = client.call(
                "Runtime.evaluate",
                {
                    "expression": expr,
                    "awaitPromise": True,
                    "returnByValue": True,
                },
                timeout_s=8.0,
            )
            result = ((response.get("result") or {}).get("result") or {}).get("value") or {}
            if isinstance(result, dict):
                last = result
                if result.get("ready") and not result.get("has_welcome"):
                    return result
        finally:
            client.close()
        time.sleep(1.0)
    raise RuntimeError(f"Dreamview main UI not ready: {last}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8888")
    parser.add_argument("--port", type=int, default=9222)
    parser.add_argument("--timeout-sec", type=float, default=45.0)
    args = parser.parse_args()
    result = auto_enter(args.url, args.port, args.timeout_sec)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

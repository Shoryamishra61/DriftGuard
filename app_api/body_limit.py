"""ASGI request-body limiter covering declared and chunked HTTP bodies."""

from __future__ import annotations

from starlette.responses import JSONResponse
from starlette.types import Message, Receive, Scope, Send


class RequestBodyLimitMiddleware:
    def __init__(self, app, max_bytes: int):
        self.app = app
        self.max_bytes = max_bytes

    @staticmethod
    def _content_length(scope: Scope) -> int | None:
        values = [value for key, value in scope.get("headers", []) if key == b"content-length"]
        if not values:
            return None
        if len(values) != 1:
            raise ValueError("multiple Content-Length headers")
        parsed = int(values[0])
        if parsed < 0:
            raise ValueError("negative Content-Length")
        return parsed

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        try:
            declared_length = self._content_length(scope)
        except (TypeError, ValueError):
            await JSONResponse(
                {"detail": "invalid Content-Length"},
                status_code=400,
            )(scope, receive, send)
            return

        if declared_length is not None and declared_length > self.max_bytes:
            await self._reject(scope, receive, send)
            return

        body_parts: list[bytes] = []
        received_bytes = 0
        more_body = True
        while more_body:
            message = await receive()
            if message["type"] == "http.disconnect":
                await self.app(scope, _disconnect_receive, send)
                return
            chunk = message.get("body", b"")
            received_bytes += len(chunk)
            if received_bytes > self.max_bytes:
                await self._reject(scope, receive, send)
                return
            body_parts.append(chunk)
            more_body = bool(message.get("more_body", False))

        replayed = False

        async def replay_receive() -> Message:
            nonlocal replayed
            if replayed:
                return {"type": "http.disconnect"}
            replayed = True
            return {
                "type": "http.request",
                "body": b"".join(body_parts),
                "more_body": False,
            }

        await self.app(scope, replay_receive, send)

    @staticmethod
    async def _reject(scope: Scope, receive: Receive, send: Send) -> None:
        del receive
        await JSONResponse(
            {"detail": "request body too large"},
            status_code=413,
        )(scope, _empty_receive, send)


async def _empty_receive() -> Message:
    return {"type": "http.request", "body": b"", "more_body": False}


async def _disconnect_receive() -> Message:
    return {"type": "http.disconnect"}

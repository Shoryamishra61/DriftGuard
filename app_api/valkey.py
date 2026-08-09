"""Valkey client construction using the Redis-compatible async protocol."""

from __future__ import annotations

from redis.asyncio import Redis

from app_api.config import ConfigurationError, Settings


def create_valkey_client(settings: Settings) -> Redis:
    common_options = {
        "decode_responses": True,
        "socket_connect_timeout": settings.dependency_timeout_seconds,
        "socket_timeout": settings.dependency_timeout_seconds,
        "health_check_interval": 30,
    }
    if settings.valkey_url is not None:
        url = settings.valkey_url.get_secret_value()
        if not url.startswith(("redis://", "rediss://")):
            raise ConfigurationError("VALKEY_URL must use redis:// or rediss://")
        return Redis.from_url(url, **common_options)

    password = (
        settings.valkey_password.get_secret_value()
        if settings.valkey_password is not None
        else None
    )
    return Redis(
        host=settings.valkey_host,
        port=settings.valkey_port,
        db=settings.valkey_database,
        password=password,
        **common_options,
    )


async def ping_valkey(client: Redis) -> None:
    pong = await client.ping()
    if pong is not True:
        raise ConnectionError("Valkey did not acknowledge PING")


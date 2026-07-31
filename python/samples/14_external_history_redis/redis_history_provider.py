# Copyright (c) Microsoft. All rights reserved.

"""A minimal Redis-backed history provider.

This is an ordinary Agent Framework ``HistoryProvider`` - nothing about it is durable-specific.
It is included in the sample rather than imported from a package to keep the sample dependency
free and to show exactly how little a "bring your own store" provider needs: read the messages
for a session id, append new ones.

The durable runtime leaves providers like this alone: the user chose where their conversation
lives, so durable supplies execution durability and stays out of the way of storage.
"""

from collections.abc import Sequence
from typing import Any

import redis.asyncio as aioredis
from agent_framework import HistoryProvider, Message


class RedisHistoryProvider(HistoryProvider):
    """Stores conversation history in a Redis list, one entry per message.

    Messages are keyed by session id, so the same session id must be used on every turn for the
    conversation to continue - which is exactly what the durable entity guarantees.
    """

    DEFAULT_SOURCE_ID = "redis_history"

    def __init__(
        self,
        redis_url: str,
        *,
        source_id: str = DEFAULT_SOURCE_ID,
        key_prefix: str = "durable_sample:history",
    ) -> None:
        """Create a Redis-backed history provider.

        Args:
            redis_url: Redis connection URL, for example ``redis://localhost:6379``.
            source_id: Unique identifier for this provider instance.
            key_prefix: Prefix for the Redis keys this provider owns.
        """
        super().__init__(source_id)
        self.key_prefix = key_prefix
        self._client: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)

    def _key(self, session_id: str | None) -> str:
        """Build the Redis key holding the history for a session.

        Args:
            session_id: The session ID to build a key for.

        Returns:
            The Redis key for this session's history.
        """
        return f"{self.key_prefix}:{session_id or 'default'}"

    async def get_messages(
        self, session_id: str | None, *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> list[Message]:
        """Read this session's messages from Redis, oldest first.

        Args:
            session_id: The session ID to retrieve messages for.
            state: Unused, since this provider keeps nothing in session state.
            **kwargs: Additional arguments (unused).

        Returns:
            The stored messages in chronological order.
        """
        stored: list[str] = await self._client.lrange(self._key(session_id), 0, -1)
        return [Message.from_json(entry) for entry in stored]

    async def save_messages(
        self,
        session_id: str | None,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Append messages to this session's Redis list.

        Args:
            session_id: The session ID to store messages for.
            messages: The messages to persist.
            state: Unused, since this provider keeps nothing in session state.
            **kwargs: Additional arguments (unused).
        """
        if not messages:
            return
        await self._client.rpush(self._key(session_id), *[message.to_json() for message in messages])

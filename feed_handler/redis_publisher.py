"""
Redis Publisher - Publishes ticks and bars to Redis Pub/Sub channels.

Channel structure:
  ticks:{symbol}         - Individual tick data (high frequency)
  bars:{symbol}:{tf}     - OHLCV bars on close (low frequency)
  status:feeds           - Handler health/stats (every 5s)

Uses aioredis with connection pooling, batched pipeline publishes,
and automatic reconnection.
"""
import os
import asyncio
import json
import logging
import time
from typing import Optional
from dataclasses import dataclass

import redis.asyncio as aioredis

from models import Tick
from tick_buffer import TickAggregator

logger = logging.getLogger(__name__)


@dataclass
class PublisherConfig:
    """Configuration for Redis publisher."""
    host: str = os.environ.get("REDIS_HOST", "localhost")
    port: int = int(os.environ.get("REDIS_PORT", "6379"))
    channel_prefix: str = os.environ.get("REDIS_CHANNEL_PREFIX", "trading")
    batch_size: int = int(os.environ.get("PUBLISH_BATCH_SIZE", "100"))
    flush_interval_ms: int = int(os.environ.get("PUBLISH_FLUSH_MS", "10"))
    reconnect_delay_s: float = 1.0
    reconnect_max_delay_s: float = 30.0
    status_interval_s: float = 5.0


class RedisPublisher:
    """
    Publishes market data to Redis Pub/Sub channels.

    Uses pipeline batching to reduce round-trips:
    - Accumulates up to `batch_size` messages
    - Flushes every `flush_interval_ms` milliseconds
    - Whichever threshold is hit first triggers a flush

    Handles connection drops with exponential backoff reconnection.
    """

    def __init__(self, config: Optional[PublisherConfig] = None):
        self._config = config or PublisherConfig()
        self._redis: Optional[aioredis.Redis] = None
        self._connected = False
        self._running = False

        # Batch queue
        self._batch: list[tuple[str, str]] = []  # (channel, payload)
        self._batch_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._status_task: Optional[asyncio.Task] = None

        # Stats
        self._messages_published = 0
        self._publish_errors = 0
        self._flushes = 0
        self._connected_feeds: list[str] = []

    @property
    def connected(self) -> bool:
        return self._connected

    def _channel(self, *parts: str) -> str:
        """Build a channel name with the configured prefix."""
        return ":".join([self._config.channel_prefix, *parts])

    async def connect(self) -> None:
        """Establish Redis connection with pool."""
        delay = self._config.reconnect_delay_s
        while True:
            try:
                self._redis = aioredis.Redis(
                    host=self._config.host,
                    port=self._config.port,
                    decode_responses=True,
                    max_connections=10,
                )
                # Verify the connection
                await self._redis.ping()
                self._connected = True
                logger.info(
                    "Connected to Redis at %s:%d",
                    self._config.host,
                    self._config.port,
                )
                return
            except (ConnectionError, OSError, aioredis.RedisError) as e:
                logger.warning("Redis connection failed (%s), retrying in %.1fs", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._config.reconnect_max_delay_s)

    async def start(self) -> None:
        """Start the flush loop and status publisher."""
        if not self._connected:
            await self.connect()
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop(), name="redis_flush")
        self._status_task = asyncio.create_task(self._status_loop(), name="redis_status")

    async def stop(self) -> None:
        """Stop publishing, flush remaining messages, close connection."""
        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        if self._status_task:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self._flush()

        if self._redis:
            await self._redis.aclose()
            self._connected = False
            logger.info("Redis publisher stopped (%d messages published)", self._messages_published)

    # ------------------------------------------------------------------
    # Public publish methods
    # ------------------------------------------------------------------

    async def publish_tick(self, tick: Tick) -> None:
        """Queue a tick for batched publishing."""
        precision = tick.price_precision
        divisor = 10 ** precision

        payload = json.dumps({
            "type": "tick",
            "symbol": tick.symbol,
            "ts": tick.timestamp_ns // 1_000_000,  # ms epoch
            "bid": tick.bid_price / divisor if tick.bid_price is not None else None,
            "ask": tick.ask_price / divisor if tick.ask_price is not None else None,
            "last": tick.trade_price / divisor if tick.trade_price is not None else None,
            "volume": tick.trade_size or 0,
        })

        channel = self._channel("ticks", tick.symbol)
        await self._enqueue(channel, payload)

    async def publish_bar(self, bar: TickAggregator.Bar) -> None:
        """Queue a completed OHLCV bar for publishing."""
        divisor = 10 ** bar.precision
        # Derive timeframe string from bar duration (default "1m")
        tf = "1m"

        payload = json.dumps({
            "type": "bar",
            "symbol": bar.symbol,
            "timeframe": tf,
            "ts": bar.timestamp_ns // 1_000_000,
            "o": bar.open / divisor,
            "h": bar.high / divisor,
            "l": bar.low / divisor,
            "c": bar.close / divisor,
            "v": bar.volume,
        })

        channel = self._channel("bars", bar.symbol, tf)
        await self._enqueue(channel, payload)

    def set_connected_feeds(self, feeds: list[str]) -> None:
        """Update the list of connected feed names for status broadcasts."""
        self._connected_feeds = feeds

    # ------------------------------------------------------------------
    # Internal batching
    # ------------------------------------------------------------------

    async def _enqueue(self, channel: str, payload: str) -> None:
        """Add a message to the batch queue, flushing if batch is full."""
        async with self._batch_lock:
            self._batch.append((channel, payload))
            if len(self._batch) >= self._config.batch_size:
                await self._flush_locked()

    async def _flush(self) -> None:
        """Acquire lock and flush."""
        async with self._batch_lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        """Flush the current batch via Redis pipeline. Caller must hold _batch_lock."""
        if not self._batch or not self._redis:
            return

        batch = self._batch
        self._batch = []

        try:
            pipe = self._redis.pipeline(transaction=False)
            for channel, payload in batch:
                pipe.publish(channel, payload)
            await pipe.execute()

            self._messages_published += len(batch)
            self._flushes += 1
        except (ConnectionError, OSError, aioredis.RedisError) as e:
            self._publish_errors += len(batch)
            logger.error("Redis publish failed (%d messages lost): %s", len(batch), e)
            # Attempt reconnect
            self._connected = False
            asyncio.create_task(self._reconnect())

    async def _reconnect(self) -> None:
        """Reconnect to Redis with exponential backoff."""
        if self._connected:
            return
        logger.info("Attempting Redis reconnect...")
        await self.connect()

    async def _flush_loop(self) -> None:
        """Background task: time-based flush."""
        interval = self._config.flush_interval_ms / 1000.0
        while self._running:
            try:
                await asyncio.sleep(interval)
                await self._flush()
            except asyncio.CancelledError:
                break

    async def _status_loop(self) -> None:
        """Publish handler health/stats every N seconds."""
        while self._running:
            try:
                await asyncio.sleep(self._config.status_interval_s)
                if not self._redis or not self._connected:
                    continue

                payload = json.dumps({
                    "type": "status",
                    "connected": True,
                    "feeds": self._connected_feeds,
                    "messages_published": self._messages_published,
                    "publish_errors": self._publish_errors,
                    "flushes": self._flushes,
                    "ts": int(time.time() * 1000),
                })
                await self._redis.publish(
                    self._channel("status", "feeds"),
                    payload,
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Status publish error: %s", e)

"""
High-performance tick buffer and batch processor.
Uses lock-free ring buffer design for minimal latency.
"""
import asyncio
from typing import Optional, Callable, Awaitable
from collections import deque
import time
from dataclasses import dataclass, field

from models import Tick, current_time_ns


@dataclass(slots=True)
class BufferStats:
    """Statistics for buffer monitoring."""
    ticks_received: int = 0
    ticks_processed: int = 0
    ticks_dropped: int = 0
    batches_flushed: int = 0
    max_latency_ns: int = 0
    avg_latency_ns: int = 0


class RingBuffer:
    """
    Lock-free single-producer single-consumer ring buffer.
    
    Optimized for:
    - Zero allocation after initialization
    - Cache-friendly sequential access
    - Minimal contention between reader/writer
    """
    
    def __init__(self, capacity: int = 65536):
        """
        Initialize ring buffer.
        
        Args:
            capacity: Must be power of 2 for efficient modulo
        """
        # Ensure capacity is power of 2
        self._capacity = 1 << (capacity - 1).bit_length()
        self._mask = self._capacity - 1
        
        # Pre-allocate buffer
        self._buffer: list[Optional[Tick]] = [None] * self._capacity
        
        # Separate cache lines for read/write indices to avoid false sharing
        self._write_idx = 0
        self._read_idx = 0
    
    @property
    def size(self) -> int:
        """Current number of items in buffer."""
        return (self._write_idx - self._read_idx) & self._mask
    
    @property
    def is_full(self) -> bool:
        return self.size == self._capacity - 1
    
    @property
    def is_empty(self) -> bool:
        return self._write_idx == self._read_idx
    
    def push(self, tick: Tick) -> bool:
        """
        Push tick to buffer.
        
        Returns:
            True if successful, False if buffer full
        """
        next_write = (self._write_idx + 1) & self._mask
        
        if next_write == self._read_idx:
            return False  # Buffer full
        
        self._buffer[self._write_idx] = tick
        self._write_idx = next_write
        return True
    
    def pop(self) -> Optional[Tick]:
        """
        Pop tick from buffer.
        
        Returns:
            Tick if available, None if buffer empty
        """
        if self._read_idx == self._write_idx:
            return None  # Buffer empty
        
        tick = self._buffer[self._read_idx]
        self._buffer[self._read_idx] = None  # Help GC
        self._read_idx = (self._read_idx + 1) & self._mask
        return tick
    
    def pop_batch(self, max_size: int) -> list[Tick]:
        """
        Pop multiple ticks efficiently.
        
        Returns:
            List of ticks (up to max_size)
        """
        batch = []
        count = min(max_size, self.size)
        
        for _ in range(count):
            tick = self.pop()
            if tick:
                batch.append(tick)
        
        return batch


class TickBuffer:
    """
    Buffered tick processor with batching support.
    
    Features:
    - Ring buffer for low-latency queueing
    - Configurable batch sizes
    - Time-based and count-based flushing
    - Backpressure handling
    """
    
    def __init__(
        self,
        on_batch: Callable[[list[Tick]], Awaitable[None]],
        batch_size: int = 1000,
        flush_interval_ms: int = 100,
        buffer_capacity: int = 65536,
    ):
        """
        Initialize tick buffer.
        
        Args:
            on_batch: Async callback for batch processing
            batch_size: Max ticks per batch
            flush_interval_ms: Max time between flushes
            buffer_capacity: Ring buffer capacity
        """
        self._on_batch = on_batch
        self._batch_size = batch_size
        self._flush_interval_ns = flush_interval_ms * 1_000_000
        self._buffer = RingBuffer(buffer_capacity)
        
        self._stats = BufferStats()
        self._last_flush_time = current_time_ns()
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None
    
    @property
    def stats(self) -> BufferStats:
        return self._stats
    
    async def start(self) -> None:
        """Start the background flush task."""
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
    
    async def stop(self) -> None:
        """Stop and flush remaining ticks."""
        self._running = False
        
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Final flush
        await self._flush()
    
    async def push(self, tick: Tick) -> bool:
        """
        Push tick to buffer.
        
        Returns:
            True if successful, False if dropped
        """
        self._stats.ticks_received += 1
        
        if not self._buffer.push(tick):
            self._stats.ticks_dropped += 1
            return False
        
        # Check if we should flush immediately (batch full)
        if self._buffer.size >= self._batch_size:
            await self._flush()
        
        return True
    
    async def _flush_loop(self) -> None:
        """Background task for time-based flushing."""
        while self._running:
            try:
                await asyncio.sleep(self._flush_interval_ns / 1_000_000_000)
                
                if not self._buffer.is_empty:
                    await self._flush()
                    
            except asyncio.CancelledError:
                break
    
    async def _flush(self) -> None:
        """Flush buffered ticks to callback."""
        batch = self._buffer.pop_batch(self._batch_size)
        
        if not batch:
            return
        
        now = current_time_ns()
        
        # Calculate latency (time from oldest tick to flush)
        oldest_tick = batch[0]
        latency = now - oldest_tick.timestamp_ns
        
        self._stats.max_latency_ns = max(self._stats.max_latency_ns, latency)
        self._stats.avg_latency_ns = int(
            0.9 * self._stats.avg_latency_ns + 0.1 * latency
        )
        
        # Process batch
        await self._on_batch(batch)
        
        self._stats.ticks_processed += len(batch)
        self._stats.batches_flushed += 1
        self._last_flush_time = now


class TickAggregator:
    """
    Aggregates ticks into OHLCV bars in real-time.
    
    Features:
    - Multiple timeframes (1s, 1m, 5m, etc.)
    - Memory-efficient incremental updates
    - Callback on bar completion
    """
    
    @dataclass
    class Bar:
        """OHLCV bar."""
        timestamp_ns: int
        symbol: str
        open: int
        high: int
        low: int
        close: int
        volume: int = 0
        tick_count: int = 0
        precision: int = 2
    
    def __init__(
        self,
        timeframe_seconds: int = 60,
        on_bar: Optional[Callable[['TickAggregator.Bar'], Awaitable[None]]] = None,
    ):
        self._timeframe_ns = timeframe_seconds * 1_000_000_000
        self._on_bar = on_bar
        
        # Current bars by symbol
        self._bars: dict[str, TickAggregator.Bar] = {}
    
    def _get_bar_timestamp(self, tick_time_ns: int) -> int:
        """Round timestamp down to bar boundary."""
        return (tick_time_ns // self._timeframe_ns) * self._timeframe_ns
    
    async def process_tick(self, tick: Tick) -> Optional['TickAggregator.Bar']:
        """
        Process tick and update bar.
        
        Returns:
            Completed bar if a bar was closed, None otherwise
        """
        bar_ts = self._get_bar_timestamp(tick.timestamp_ns)
        symbol = tick.symbol
        
        # Get price from tick
        price = tick.trade_price or tick.bid_price or tick.ask_price
        if price is None:
            return None
        
        size = tick.trade_size or 0
        
        # Check if we need to close current bar and start new one
        completed_bar = None
        
        if symbol in self._bars:
            current_bar = self._bars[symbol]
            
            if bar_ts > current_bar.timestamp_ns:
                # Bar completed
                completed_bar = current_bar
                
                if self._on_bar:
                    await self._on_bar(completed_bar)
                
                # Start new bar
                self._bars[symbol] = self.Bar(
                    timestamp_ns=bar_ts,
                    symbol=symbol,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=size,
                    tick_count=1,
                    precision=tick.price_precision,
                )
            else:
                # Update current bar
                current_bar.high = max(current_bar.high, price)
                current_bar.low = min(current_bar.low, price)
                current_bar.close = price
                current_bar.volume += size
                current_bar.tick_count += 1
        else:
            # First tick for this symbol
            self._bars[symbol] = self.Bar(
                timestamp_ns=bar_ts,
                symbol=symbol,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=size,
                tick_count=1,
                precision=tick.price_precision,
            )
        
        return completed_bar
    
    def get_current_bar(self, symbol: str) -> Optional['TickAggregator.Bar']:
        """Get current (incomplete) bar for symbol."""
        return self._bars.get(symbol)
    
    async def flush_all(self) -> list['TickAggregator.Bar']:
        """Flush all current bars (e.g., at end of session)."""
        bars = list(self._bars.values())
        
        if self._on_bar:
            for bar in bars:
                await self._on_bar(bar)
        
        self._bars.clear()
        return bars

"""
Core data models for market data feed handler.
Uses dataclasses and __slots__ for memory efficiency.
"""
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional
import time


class TickType(Enum):
    TRADE = auto()
    QUOTE = auto()
    BBO = auto()  # Best Bid/Offer


class Vendor(Enum):
    DATABENTO = "databento"
    BLOOMBERG = "bloomberg"
    CME = "cme"
    ICE = "ice"
    REFINITIV = "refinitiv"


@dataclass(slots=True, frozen=True)
class Tick:
    """
    Immutable tick data structure.
    Using __slots__ via slots=True for ~40% memory reduction.
    frozen=True makes it hashable and thread-safe.
    """
    timestamp_ns: int          # Nanosecond precision timestamp
    symbol: str                # Normalized symbol (e.g., "ES.CME", "EURUSD.CME")
    tick_type: TickType
    
    # Price fields (using int for price to avoid float precision issues)
    # Store as price * 10^precision, e.g., 4532.25 -> 453225 with precision=2
    bid_price: Optional[int] = None
    ask_price: Optional[int] = None
    trade_price: Optional[int] = None
    
    # Size fields
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    trade_size: Optional[int] = None
    
    # Metadata
    exchange: Optional[str] = None
    vendor: Optional[Vendor] = None
    sequence_num: Optional[int] = None  # For gap detection
    
    # Price precision (digits after decimal)
    price_precision: int = 2
    
    def get_mid_price(self) -> Optional[float]:
        """Calculate mid price from bid/ask."""
        if self.bid_price is not None and self.ask_price is not None:
            mid = (self.bid_price + self.ask_price) / 2
            return mid / (10 ** self.price_precision)
        return None
    
    def get_trade_price_float(self) -> Optional[float]:
        """Convert integer trade price to float."""
        if self.trade_price is not None:
            return self.trade_price / (10 ** self.price_precision)
        return None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            'timestamp_ns': self.timestamp_ns,
            'symbol': self.symbol,
            'tick_type': self.tick_type.name,
            'bid_price': self.bid_price,
            'ask_price': self.ask_price,
            'trade_price': self.trade_price,
            'bid_size': self.bid_size,
            'ask_size': self.ask_size,
            'trade_size': self.trade_size,
            'exchange': self.exchange,
            'vendor': self.vendor.value if self.vendor else None,
            'sequence_num': self.sequence_num,
            'price_precision': self.price_precision,
        }


@dataclass(slots=True)
class SubscriptionRequest:
    """Request to subscribe to market data."""
    symbols: list[str]
    vendor: Vendor
    tick_types: list[TickType] = field(default_factory=lambda: [TickType.QUOTE, TickType.TRADE])
    
    
@dataclass(slots=True)
class FeedStats:
    """Statistics for monitoring feed health."""
    vendor: Vendor
    symbol: str
    ticks_received: int = 0
    last_tick_time_ns: int = 0
    gaps_detected: int = 0
    last_sequence: int = 0
    latency_ns_avg: int = 0
    
    def update(self, tick: Tick, receive_time_ns: int) -> None:
        """Update stats with new tick."""
        self.ticks_received += 1
        
        # Gap detection
        if tick.sequence_num is not None:
            if self.last_sequence > 0 and tick.sequence_num != self.last_sequence + 1:
                self.gaps_detected += 1
            self.last_sequence = tick.sequence_num
        
        # Latency tracking (simple exponential moving average)
        if tick.timestamp_ns > 0:
            latency = receive_time_ns - tick.timestamp_ns
            if self.latency_ns_avg == 0:
                self.latency_ns_avg = latency
            else:
                self.latency_ns_avg = int(0.9 * self.latency_ns_avg + 0.1 * latency)
        
        self.last_tick_time_ns = receive_time_ns


def current_time_ns() -> int:
    """Get current time in nanoseconds."""
    return time.time_ns()

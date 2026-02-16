"""
Bloomberg feed handler implementation.
Uses Bloomberg's BLPAPI for market data subscription.
"""
import asyncio
from typing import AsyncIterator, Optional, Any
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import queue
import threading

from base_handler import FeedHandler, TickCallback
from models import Tick, TickType, Vendor, FeedStats, current_time_ns


class BloombergHandler(FeedHandler):
    """
    Feed handler for Bloomberg market data via BLPAPI.
    
    Bloomberg provides:
    - Real-time quotes and trades
    - Reference data
    - Historical data
    
    Note: BLPAPI is synchronous, so we run it in a thread pool
    and bridge to async with a queue.
    
    Requires: blpapi package and Bloomberg Terminal or B-PIPE connection.
    """
    
    # Bloomberg field names
    FIELD_BID = "BID"
    FIELD_ASK = "ASK"
    FIELD_LAST_PRICE = "LAST_PRICE"
    FIELD_BID_SIZE = "BID_SIZE"
    FIELD_ASK_SIZE = "ASK_SIZE"
    FIELD_VOLUME = "VOLUME"
    
    DEFAULT_FIELDS = [FIELD_BID, FIELD_ASK, FIELD_LAST_PRICE, 
                      FIELD_BID_SIZE, FIELD_ASK_SIZE]
    
    def __init__(
        self,
        on_tick: TickCallback,
        on_error: Optional[callable] = None,
        host: str = "localhost",
        port: int = 8194,
        fields: Optional[list[str]] = None,
    ):
        super().__init__(Vendor.BLOOMBERG, on_tick, on_error)
        self._host = host
        self._port = port
        self._fields = fields or self.DEFAULT_FIELDS
        
        # Thread-safe queue for cross-thread communication
        self._tick_queue: queue.Queue[Optional[Tick]] = queue.Queue(maxsize=100000)
        
        # Bloomberg session (would be blpapi.Session in real impl)
        self._session: Any = None
        self._subscription_list: Any = None
        
        # Thread pool for Bloomberg operations
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bbg")
        self._reader_thread: Optional[threading.Thread] = None
        
    async def connect(self) -> None:
        """
        Connect to Bloomberg.
        
        In production, this creates a blpapi.Session with:
        - SessionOptions for host/port
        - Event handler for async message processing
        """
        # Run blocking connection in thread pool
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, self._connect_sync)
        self._connected = True
    
    def _connect_sync(self) -> None:
        """Synchronous Bloomberg connection (runs in thread)."""
        # In real implementation:
        # import blpapi
        # options = blpapi.SessionOptions()
        # options.setServerHost(self._host)
        # options.setServerPort(self._port)
        # self._session = blpapi.Session(options, self._event_handler)
        # self._session.start()
        # self._session.openService("//blp/mktdata")
        
        # Mock for demonstration
        self._session = MockBloombergSession()
        self._session.start()
    
    async def disconnect(self) -> None:
        """Disconnect from Bloomberg."""
        if self._session:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self._executor, self._session.stop)
        
        # Signal reader thread to stop
        self._tick_queue.put(None)
        
        self._connected = False
        self._executor.shutdown(wait=False)
    
    async def subscribe(self, symbols: list[str]) -> None:
        """
        Subscribe to Bloomberg symbols.
        
        Bloomberg uses different symbology:
        - Equities: "AAPL US Equity"
        - Futures: "ESZ3 Index" or "ESZ3 Comdty"
        - FX: "EURUSD Curncy"
        """
        self._subscriptions.extend(symbols)
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self._executor, 
            self._subscribe_sync, 
            symbols
        )
        
        # Initialize stats
        for symbol in symbols:
            self._stats[symbol] = FeedStats(vendor=self.vendor, symbol=symbol)
    
    def _subscribe_sync(self, symbols: list[str]) -> None:
        """Synchronous subscription (runs in thread)."""
        # In real implementation:
        # subscriptions = blpapi.SubscriptionList()
        # for symbol in symbols:
        #     subscriptions.add(symbol, self._fields, "", 
        #                       blpapi.CorrelationId(symbol))
        # self._session.subscribe(subscriptions)
        
        # Mock
        self._session.subscribe(symbols, self._fields)
        
        # Start reader thread
        if not self._reader_thread or not self._reader_thread.is_alive():
            self._reader_thread = threading.Thread(
                target=self._read_events_sync,
                daemon=True
            )
            self._reader_thread.start()
    
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from symbols."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self._executor,
            self._unsubscribe_sync,
            symbols
        )
        
        for symbol in symbols:
            if symbol in self._subscriptions:
                self._subscriptions.remove(symbol)
    
    def _unsubscribe_sync(self, symbols: list[str]) -> None:
        """Synchronous unsubscribe (runs in thread)."""
        if self._session:
            self._session.unsubscribe(symbols)
    
    def _read_events_sync(self) -> None:
        """
        Read Bloomberg events in a background thread.
        
        Bloomberg uses an event-driven model where we poll for events
        and process them based on event type.
        """
        while self._running and self._session:
            try:
                # In real implementation:
                # event = self._session.nextEvent(timeout=1000)
                # for msg in event:
                #     self._process_message(msg)
                
                # Mock
                event = self._session.nextEvent(timeout=1000)
                if event:
                    tick = self._parse_event(event)
                    if tick:
                        try:
                            self._tick_queue.put(tick, timeout=1.0)
                        except queue.Full:
                            pass  # Drop tick if queue full (backpressure)
                            
            except Exception:
                continue
    
    def _parse_event(self, event: dict) -> Optional[Tick]:
        """Parse Bloomberg event into Tick."""
        if not event or event.get("type") != "SUBSCRIPTION_DATA":
            return None
        
        symbol = event.get("symbol", "")
        timestamp = current_time_ns()
        
        # Extract prices
        bid = event.get(self.FIELD_BID)
        ask = event.get(self.FIELD_ASK)
        last = event.get(self.FIELD_LAST_PRICE)
        
        precision = 4  # Bloomberg typically uses 4 decimal places
        multiplier = 10 ** precision
        
        # Determine tick type
        if last is not None:
            tick_type = TickType.TRADE
        elif bid is not None and ask is not None:
            tick_type = TickType.BBO
        else:
            tick_type = TickType.QUOTE
        
        return Tick(
            timestamp_ns=timestamp,
            symbol=symbol,
            tick_type=tick_type,
            bid_price=int(bid * multiplier) if bid else None,
            ask_price=int(ask * multiplier) if ask else None,
            trade_price=int(last * multiplier) if last else None,
            bid_size=event.get(self.FIELD_BID_SIZE),
            ask_size=event.get(self.FIELD_ASK_SIZE),
            vendor=Vendor.BLOOMBERG,
            price_precision=precision,
        )
    
    async def _read_messages(self) -> AsyncIterator[Tick]:
        """
        Async generator that reads from the tick queue.
        
        This bridges the synchronous Bloomberg thread to the async world.
        """
        while self._running:
            try:
                # Non-blocking check with asyncio
                loop = asyncio.get_event_loop()
                tick = await loop.run_in_executor(
                    None,
                    lambda: self._tick_queue.get(timeout=1.0)
                )
                
                if tick is None:
                    break
                
                # Update stats
                receive_time = current_time_ns()
                if tick.symbol in self._stats:
                    self._stats[tick.symbol].update(tick, receive_time)
                
                yield tick
                
            except queue.Empty:
                continue
            except Exception:
                continue


class MockBloombergSession:
    """Mock Bloomberg session for testing."""
    
    def __init__(self):
        self._running = False
        self._symbols: list[str] = []
        self._event_counter = 0
    
    def start(self) -> None:
        self._running = True
    
    def stop(self) -> None:
        self._running = False
    
    def subscribe(self, symbols: list[str], fields: list[str]) -> None:
        self._symbols = symbols
    
    def unsubscribe(self, symbols: list[str]) -> None:
        for s in symbols:
            if s in self._symbols:
                self._symbols.remove(s)
    
    def nextEvent(self, timeout: int = 1000) -> Optional[dict]:
        """Generate mock market data events."""
        import random
        import time
        
        if not self._running or not self._symbols:
            time.sleep(timeout / 1000)
            return None
        
        # Simulate some delay
        time.sleep(0.01)  # 10ms
        
        self._event_counter += 1
        symbol = random.choice(self._symbols)
        
        # Generate mock tick
        base_price = 4500.0 + random.random() * 100
        spread = 0.25
        
        return {
            "type": "SUBSCRIPTION_DATA",
            "symbol": symbol,
            "BID": base_price,
            "ASK": base_price + spread,
            "LAST_PRICE": base_price + spread / 2 if random.random() > 0.5 else None,
            "BID_SIZE": random.randint(10, 500),
            "ASK_SIZE": random.randint(10, 500),
        }

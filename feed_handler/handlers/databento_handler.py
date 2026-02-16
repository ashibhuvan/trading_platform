"""
Databento feed handler implementation.
Databento provides normalized market data via WebSocket/TCP.
"""
import asyncio
import json
from typing import AsyncIterator, Optional
import struct

from base_handler import FeedHandler, TickCallback
from models import Tick, TickType, Vendor, FeedStats, current_time_ns


class DatabentoHandler(FeedHandler):
    """
    Feed handler for Databento market data.
    
    Databento provides:
    - Normalized data across exchanges (CME, ICE, etc.)
    - Multiple schemas: MBO (L3), MBP (L2), Trades, OHLCV
    - Sub-microsecond timestamps
    
    Connection: WebSocket or TCP with their DBN binary format.
    """
    
    # Databento schema types
    SCHEMA_MBP_1 = "mbp-1"      # Top of book
    SCHEMA_MBP_10 = "mbp-10"    # 10 levels
    SCHEMA_TRADES = "trades"    # Trade ticks
    SCHEMA_OHLCV = "ohlcv-1s"   # 1-second bars
    
    def __init__(
        self,
        api_key: str,
        dataset: str,  # e.g., "GLBX.MDP3" for CME
        on_tick: TickCallback,
        on_error: Optional[callable] = None,
        schema: str = SCHEMA_MBP_1,
        host: str = "localhost",  # For demo; real: "live.databento.com"
        port: int = 13000,
    ):
        super().__init__(Vendor.DATABENTO, on_tick, on_error)
        self._api_key = api_key
        self._dataset = dataset
        self._schema = schema
        self._host = host
        self._port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._buffer = bytearray()
    
    async def connect(self) -> None:
        """Connect to Databento live gateway."""
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )
        
        # Send authentication
        auth_msg = {
            "type": "auth",
            "key": self._api_key,
            "dataset": self._dataset,
            "schema": self._schema,
        }
        await self._send_json(auth_msg)
        
        # Wait for auth response
        response = await self._read_json()
        if response.get("status") != "ok":
            raise ConnectionError(f"Databento auth failed: {response}")
        
        self._connected = True
    
    async def disconnect(self) -> None:
        """Disconnect from Databento."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        self._connected = False
    
    async def subscribe(self, symbols: list[str]) -> None:
        """Subscribe to symbols."""
        self._subscriptions.extend(symbols)
        
        sub_msg = {
            "type": "subscribe",
            "symbols": symbols,
        }
        await self._send_json(sub_msg)
        
        # Initialize stats for each symbol
        for symbol in symbols:
            self._stats[symbol] = FeedStats(
                vendor=self.vendor,
                symbol=symbol
            )
    
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from symbols."""
        unsub_msg = {
            "type": "unsubscribe",
            "symbols": symbols,
        }
        await self._send_json(unsub_msg)
        
        for symbol in symbols:
            if symbol in self._subscriptions:
                self._subscriptions.remove(symbol)
    
    async def _send_json(self, msg: dict) -> None:
        """Send JSON message."""
        if self._writer:
            data = json.dumps(msg).encode() + b"\n"
            self._writer.write(data)
            await self._writer.drain()
    
    async def _read_json(self) -> dict:
        """Read JSON response."""
        if self._reader:
            line = await self._reader.readline()
            return json.loads(line.decode())
        return {}
    
    async def _read_messages(self) -> AsyncIterator[Tick]:
        """
        Read and parse Databento messages.
        
        In production, Databento uses DBN binary format for efficiency.
        This is a simplified JSON-based implementation for demonstration.
        """
        if not self._reader:
            return
        
        while self._running:
            try:
                line = await asyncio.wait_for(
                    self._reader.readline(),
                    timeout=30.0  # Heartbeat timeout
                )
                
                if not line:
                    break
                
                receive_time = current_time_ns()
                msg = json.loads(line.decode())
                
                # Parse based on message type
                tick = self._parse_message(msg, receive_time)
                if tick:
                    # Update stats
                    if tick.symbol in self._stats:
                        self._stats[tick.symbol].update(tick, receive_time)
                    yield tick
                    
            except asyncio.TimeoutError:
                # Send heartbeat or handle stale connection
                continue
            except json.JSONDecodeError:
                continue
    
    def _parse_message(self, msg: dict, receive_time: int) -> Optional[Tick]:
        """
        Parse Databento message into Tick.
        
        Databento MBP-1 message example:
        {
            "ts_event": 1699574400123456789,
            "symbol": "ESZ3",
            "bid_px": 4532.25,
            "ask_px": 4532.50,
            "bid_sz": 150,
            "ask_sz": 200,
            "sequence": 12345
        }
        """
        msg_type = msg.get("type", "mbp")
        
        if msg_type == "heartbeat":
            return None
        
        symbol = msg.get("symbol", "")
        timestamp = msg.get("ts_event", receive_time)
        
        # Determine tick type
        if "trade_px" in msg:
            tick_type = TickType.TRADE
        elif "bid_px" in msg and "ask_px" in msg:
            tick_type = TickType.BBO
        else:
            tick_type = TickType.QUOTE
        
        # Convert prices to integer representation (avoid float issues)
        precision = 2
        multiplier = 10 ** precision
        
        bid_price = int(msg["bid_px"] * multiplier) if "bid_px" in msg else None
        ask_price = int(msg["ask_px"] * multiplier) if "ask_px" in msg else None
        trade_price = int(msg["trade_px"] * multiplier) if "trade_px" in msg else None
        
        return Tick(
            timestamp_ns=timestamp,
            symbol=symbol,
            tick_type=tick_type,
            bid_price=bid_price,
            ask_price=ask_price,
            trade_price=trade_price,
            bid_size=msg.get("bid_sz"),
            ask_size=msg.get("ask_sz"),
            trade_size=msg.get("trade_sz"),
            exchange=msg.get("exchange"),
            vendor=Vendor.DATABENTO,
            sequence_num=msg.get("sequence"),
            price_precision=precision,
        )


class DatabentoHandlerBinary(DatabentoHandler):
    """
    High-performance binary protocol handler.
    
    This demonstrates how you'd handle Databento's actual DBN format
    for maximum throughput. Uses struct for zero-copy parsing.
    """
    
    # DBN record header format (simplified)
    # Real DBN has more complex structure
    HEADER_FORMAT = "<QIH"  # timestamp(8), length(4), rtype(2)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    
    # MBP-1 record format
    MBP1_FORMAT = "<qqqIIBB"  # bid_px, ask_px, trade_px, bid_sz, ask_sz, flags, _pad
    
    async def _read_messages(self) -> AsyncIterator[Tick]:
        """Read binary DBN messages with zero-copy parsing."""
        if not self._reader:
            return
        
        while self._running:
            try:
                # Read header
                header_data = await self._reader.readexactly(self.HEADER_SIZE)
                timestamp, length, rtype = struct.unpack(self.HEADER_FORMAT, header_data)
                
                # Read body
                body_data = await self._reader.readexactly(length - self.HEADER_SIZE)
                receive_time = current_time_ns()
                
                # Parse based on record type
                tick = self._parse_binary_record(rtype, timestamp, body_data, receive_time)
                if tick:
                    yield tick
                    
            except asyncio.IncompleteReadError:
                break
            except Exception:
                continue
    
    def _parse_binary_record(
        self, 
        rtype: int, 
        timestamp: int, 
        data: bytes,
        receive_time: int
    ) -> Optional[Tick]:
        """Parse binary record into Tick."""
        # This is a simplified example
        # Real implementation would handle all DBN record types
        if len(data) < struct.calcsize(self.MBP1_FORMAT):
            return None
        
        values = struct.unpack(self.MBP1_FORMAT, data[:struct.calcsize(self.MBP1_FORMAT)])
        bid_px, ask_px, trade_px, bid_sz, ask_sz, flags, _ = values
        
        # DBN uses fixed-point prices (price * 1e9)
        precision = 9
        
        return Tick(
            timestamp_ns=timestamp,
            symbol="",  # Would come from symbol map
            tick_type=TickType.BBO,
            bid_price=bid_px,
            ask_price=ask_px,
            trade_price=trade_px if trade_px != 0 else None,
            bid_size=bid_sz,
            ask_size=ask_sz,
            vendor=Vendor.DATABENTO,
            price_precision=precision,
        )

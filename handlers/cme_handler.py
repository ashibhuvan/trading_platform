"""
Direct exchange feed handler - CME MDP 3.0 implementation.
This handles multicast UDP market data directly from CME.
"""
import asyncio
import socket
import struct
from typing import AsyncIterator, Optional
from dataclasses import dataclass

from base_handler import FeedHandler, TickCallback
from models import Tick, TickType, Vendor, FeedStats, current_time_ns


@dataclass(slots=True)
class CMEPacketHeader:
    """CME MDP 3.0 packet header."""
    msg_seq_num: int
    sending_time: int


@dataclass(slots=True)
class CMEMessageHeader:
    """CME MDP 3.0 message header."""
    msg_size: int
    block_length: int
    template_id: int
    schema_id: int
    version: int


class CMEHandler(FeedHandler):
    """
    Direct CME MDP 3.0 market data feed handler.
    
    CME distributes market data via multicast UDP. Key concepts:
    - Incremental feed: Real-time updates (deltas)
    - Snapshot feed: Full book recovery
    - Definition feed: Instrument definitions
    
    This implementation handles:
    - Multicast UDP reception
    - SBE (Simple Binary Encoding) message parsing
    - Sequence number gap detection
    - Book state management
    
    Real deployment requires:
    - Multicast network connectivity to CME
    - Market data license
    - Co-location for lowest latency
    """
    
    # CME MDP 3.0 template IDs (subset)
    TEMPLATE_MD_INCREMENTAL_REFRESH = 32
    TEMPLATE_MD_SNAPSHOT_FULL_REFRESH = 38
    TEMPLATE_SECURITY_STATUS = 30
    
    # Entry types
    ENTRY_TYPE_BID = ord('0')
    ENTRY_TYPE_OFFER = ord('1')
    ENTRY_TYPE_TRADE = ord('2')
    
    # Packet header format: seq_num(4) + sending_time(8)
    PACKET_HEADER_FORMAT = "<IQ"
    PACKET_HEADER_SIZE = struct.calcsize(PACKET_HEADER_FORMAT)
    
    # Message header format: msg_size(2) + block_length(2) + template_id(2) + schema_id(2) + version(2)
    MSG_HEADER_FORMAT = "<HHHHH"
    MSG_HEADER_SIZE = struct.calcsize(MSG_HEADER_FORMAT)
    
    def __init__(
        self,
        on_tick: TickCallback,
        on_error: Optional[callable] = None,
        multicast_group: str = "224.0.28.1",  # CME incremental feed A
        port: int = 14310,
        interface: str = "",  # Network interface IP
        snapshot_group: str = "224.0.27.1",
        snapshot_port: int = 15310,
    ):
        super().__init__(Vendor.CME, on_tick, on_error)
        self._multicast_group = multicast_group
        self._port = port
        self._interface = interface
        self._snapshot_group = snapshot_group
        self._snapshot_port = snapshot_port
        
        self._socket: Optional[socket.socket] = None
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional['CMEProtocol'] = None
        
        # Sequence tracking for gap detection
        self._expected_seq: int = 0
        self._gaps: list[tuple[int, int]] = []  # List of (start, end) gaps
        
        # Security ID to symbol mapping (from definition feed)
        self._security_map: dict[int, str] = {}
        
        # Order book state (simplified)
        self._books: dict[str, dict] = {}
    
    async def connect(self) -> None:
        """Join multicast group and start receiving."""
        loop = asyncio.get_event_loop()
        
        # Create protocol instance
        self._protocol = CMEProtocol(self)
        
        # Create UDP endpoint with multicast
        transport, _ = await loop.create_datagram_endpoint(
            lambda: self._protocol,
            local_addr=(self._interface or '0.0.0.0', self._port),
            family=socket.AF_INET,
            reuse_port=True,
        )
        self._transport = transport
        
        # Join multicast group
        sock = transport.get_extra_info('socket')
        if sock:
            # Set up multicast membership
            mreq = struct.pack(
                '4s4s',
                socket.inet_aton(self._multicast_group),
                socket.inet_aton(self._interface or '0.0.0.0')
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            # Optimize socket for low latency
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
        
        self._connected = True
    
    async def disconnect(self) -> None:
        """Leave multicast group and close socket."""
        if self._transport:
            self._transport.close()
        self._transport = None
        self._protocol = None
        self._connected = False
    
    async def subscribe(self, symbols: list[str]) -> None:
        """
        Subscribe to symbols.
        
        For CME, subscription is passive - we receive all instruments
        on the multicast channel. Filtering is done client-side.
        """
        self._subscriptions.extend(symbols)
        
        for symbol in symbols:
            self._stats[symbol] = FeedStats(vendor=self.vendor, symbol=symbol)
            self._books[symbol] = {'bids': {}, 'asks': {}}
    
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe (stop processing) symbols."""
        for symbol in symbols:
            if symbol in self._subscriptions:
                self._subscriptions.remove(symbol)
            if symbol in self._books:
                del self._books[symbol]
    
    async def _read_messages(self) -> AsyncIterator[Tick]:
        """Read ticks from the protocol's queue."""
        if not self._protocol:
            return
        
        while self._running:
            try:
                tick = await asyncio.wait_for(
                    self._protocol.tick_queue.get(),
                    timeout=5.0
                )
                
                if tick and tick.symbol in self._subscriptions:
                    receive_time = current_time_ns()
                    if tick.symbol in self._stats:
                        self._stats[tick.symbol].update(tick, receive_time)
                    yield tick
                    
            except asyncio.TimeoutError:
                continue
    
    def parse_packet(self, data: bytes) -> list[Tick]:
        """
        Parse CME MDP 3.0 packet.
        
        Packet structure:
        - Packet header (12 bytes)
        - One or more SBE messages
        """
        if len(data) < self.PACKET_HEADER_SIZE:
            return []
        
        # Parse packet header
        seq_num, sending_time = struct.unpack(
            self.PACKET_HEADER_FORMAT,
            data[:self.PACKET_HEADER_SIZE]
        )
        
        # Gap detection
        if self._expected_seq > 0 and seq_num != self._expected_seq:
            if seq_num > self._expected_seq:
                self._gaps.append((self._expected_seq, seq_num - 1))
        self._expected_seq = seq_num + 1
        
        # Parse messages
        ticks = []
        offset = self.PACKET_HEADER_SIZE
        
        while offset + self.MSG_HEADER_SIZE <= len(data):
            # Parse message header
            msg_size, block_length, template_id, schema_id, version = struct.unpack(
                self.MSG_HEADER_FORMAT,
                data[offset:offset + self.MSG_HEADER_SIZE]
            )
            
            if msg_size == 0:
                break
            
            # Parse message body based on template
            msg_data = data[offset + self.MSG_HEADER_SIZE:offset + msg_size]
            
            if template_id == self.TEMPLATE_MD_INCREMENTAL_REFRESH:
                ticks.extend(self._parse_incremental_refresh(msg_data, sending_time))
            
            offset += msg_size
        
        return ticks
    
    def _parse_incremental_refresh(self, data: bytes, timestamp: int) -> list[Tick]:
        """
        Parse MD Incremental Refresh message.
        
        This is simplified - real implementation handles:
        - Multiple entry groups (MD entries)
        - Order book updates (add/modify/delete)
        - Trade messages
        - Statistics
        """
        ticks = []
        
        # Simplified parsing - in reality this is more complex
        # with repeating groups and variable-length fields
        if len(data) < 20:
            return ticks
        
        # Example: Extract first entry (simplified)
        # Real format has repeating groups with entry counts
        try:
            # Mock parsing for demonstration
            entry_type = data[0]
            security_id = struct.unpack('<I', data[1:5])[0]
            price_mantissa = struct.unpack('<q', data[5:13])[0]
            size = struct.unpack('<I', data[13:17])[0]
            
            symbol = self._security_map.get(security_id, f"SEC_{security_id}")
            
            # Price is in fixed-point with exponent -7
            price = price_mantissa  # Keep as integer, precision=7
            
            if entry_type == self.ENTRY_TYPE_BID:
                tick = Tick(
                    timestamp_ns=timestamp,
                    symbol=symbol,
                    tick_type=TickType.QUOTE,
                    bid_price=price,
                    bid_size=size,
                    vendor=Vendor.CME,
                    price_precision=7,
                )
            elif entry_type == self.ENTRY_TYPE_OFFER:
                tick = Tick(
                    timestamp_ns=timestamp,
                    symbol=symbol,
                    tick_type=TickType.QUOTE,
                    ask_price=price,
                    ask_size=size,
                    vendor=Vendor.CME,
                    price_precision=7,
                )
            elif entry_type == self.ENTRY_TYPE_TRADE:
                tick = Tick(
                    timestamp_ns=timestamp,
                    symbol=symbol,
                    tick_type=TickType.TRADE,
                    trade_price=price,
                    trade_size=size,
                    vendor=Vendor.CME,
                    price_precision=7,
                )
            else:
                return ticks
            
            ticks.append(tick)
            
        except Exception:
            pass
        
        return ticks
    
    async def request_snapshot(self, symbols: list[str]) -> None:
        """
        Request snapshot from snapshot feed for book recovery.
        
        Used when gaps are detected in the incremental feed.
        """
        # In production, this would:
        # 1. Join snapshot multicast group
        # 2. Wait for full book snapshot
        # 3. Apply snapshot to book state
        # 4. Continue processing incremental updates
        pass


class CMEProtocol(asyncio.DatagramProtocol):
    """Asyncio protocol for CME UDP reception."""
    
    def __init__(self, handler: CMEHandler):
        self.handler = handler
        self.tick_queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=100000)
    
    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        pass
    
    def datagram_received(self, data: bytes, addr: tuple) -> None:
        """Called when UDP packet is received."""
        ticks = self.handler.parse_packet(data)
        for tick in ticks:
            try:
                self.tick_queue.put_nowait(tick)
            except asyncio.QueueFull:
                pass  # Backpressure - drop oldest or newest
    
    def error_received(self, exc: Exception) -> None:
        pass
    
    def connection_lost(self, exc: Optional[Exception]) -> None:
        pass

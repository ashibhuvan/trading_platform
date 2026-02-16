#!/usr/bin/env python3
"""
High-Performance Market Data Feed Handler
==========================================

A production-grade Python system for ingesting live tick data from multiple
market data vendors (Databento, Bloomberg, CME direct).

Architecture:
- Async/await for high concurrency
- Lock-free ring buffers for low latency
- Vendor abstraction for unified tick format
- Batch processing for throughput
- Real-time OHLCV aggregation

Usage:
    python main.py --vendors databento,bloomberg --symbols ESZ3,NQZ3

Author: Interview Demo
"""

import asyncio
import argparse
import logging
import signal
from typing import Optional
from datetime import datetime

# Local imports
from models import Tick, Vendor, TickType, current_time_ns
from feed_manager import FeedManager, FeedConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class TickPersister:
    """
    Example tick persistence layer.
    
    In production, this would write to:
    - TimescaleDB for tick storage
    - Redis for real-time distribution
    - Kafka for downstream systems
    """
    
    def __init__(self, db_connection_string: Optional[str] = None):
        self._db_connection_string = db_connection_string
        self._tick_count = 0
        self._last_log_time = current_time_ns()
        self._log_interval_ns = 5_000_000_000  # 5 seconds
    
    async def process_batch(self, ticks: list[Tick]) -> None:
        """
        Process a batch of ticks.
        
        In production:
        - Validate ticks
        - Convert to database format
        - Batch insert to TimescaleDB
        - Publish to Redis pub/sub
        """
        self._tick_count += len(ticks)
        
        now = current_time_ns()
        if now - self._last_log_time > self._log_interval_ns:
            rate = self._tick_count / ((now - self._last_log_time) / 1_000_000_000)
            logger.info(f"Processed {self._tick_count} ticks ({rate:.0f}/sec)")
            
            # Log sample tick
            if ticks:
                sample = ticks[-1]
                logger.info(
                    f"Sample: {sample.symbol} "
                    f"bid={sample.bid_price} ask={sample.ask_price} "
                    f"trade={sample.trade_price}"
                )
            
            self._tick_count = 0
            self._last_log_time = now
    
    async def write_to_db(self, ticks: list[Tick]) -> None:
        """
        Write ticks to TimescaleDB.
        
        Example SQL (using asyncpg):
        ```sql
        INSERT INTO tick_data (time, symbol, bid, ask, trade, bid_size, ask_size, trade_size)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ```
        """
        # Placeholder - would use asyncpg or psycopg in production
        pass


class MarketDataApp:
    """
    Main application class.
    
    Orchestrates the feed handlers and provides a clean interface
    for starting/stopping the market data collection.
    """
    
    def __init__(self):
        self._manager: Optional[FeedManager] = None
        self._persister: Optional[TickPersister] = None
        self._shutdown_event = asyncio.Event()
    
    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown on SIGINT/SIGTERM."""
        import sys
        
        if sys.platform != 'win32':
            # Unix: use proper signal handlers
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self._shutdown())
                )
        # Windows: Ctrl+C will raise KeyboardInterrupt, handled in run()
    
    async def _shutdown(self) -> None:
        """Handle graceful shutdown."""
        logger.info("Shutdown signal received...")
        self._shutdown_event.set()
    
    async def run(
        self,
        configs: list[FeedConfig],
        enable_persistence: bool = True,
        enable_aggregation: bool = True,
        aggregation_timeframe: int = 60,
    ) -> None:
        """
        Run the market data application.
        
        Args:
            configs: List of feed configurations
            enable_persistence: Whether to enable tick persistence
            enable_aggregation: Whether to enable OHLCV aggregation
            aggregation_timeframe: Bar timeframe in seconds
        """
        self._setup_signal_handlers()
        
        # Create persister
        self._persister = TickPersister()
        
        # Create feed manager
        self._manager = FeedManager(
            on_batch=self._persister.process_batch,
            buffer_size=131072,  # 128K ticks
            batch_size=5000,
            flush_interval_ms=50,
        )
        
        # Enable aggregation if requested
        if enable_aggregation:
            async def on_bar(bar) -> None:
                logger.info(
                    f"Bar: {bar.symbol} "
                    f"O={bar.open} H={bar.high} L={bar.low} C={bar.close} "
                    f"V={bar.volume} ticks={bar.tick_count}"
                )
            
            self._manager.enable_aggregation(
                timeframe_seconds=aggregation_timeframe,
                on_bar=on_bar,
            )
        
        # Add feeds
        for config in configs:
            self._manager.add_feed(config)
        
        logger.info(f"Starting {len(configs)} feeds...")
        
        # Start feeds
        await self._manager.start()
        
        # Wait for shutdown signal
        try:
            # Print status periodically
            while not self._shutdown_event.is_set():
                await asyncio.sleep(10)
                
                stats = self._manager.get_stats()
                logger.info(
                    f"Stats: {stats['total_ticks']} ticks, "
                    f"{stats['ticks_per_second']:.0f}/sec, "
                    f"{stats['feeds_connected']}/{stats['feeds_total']} feeds connected"
                )
                
                # Print per-feed status
                for vendor, status in self._manager.get_all_status().items():
                    logger.info(
                        f"  {vendor.value}: {status.state.name}, "
                        f"{status.ticks_received} ticks, "
                        f"latency={status.latency_avg_us}μs"
                    )
                    
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            # Graceful shutdown
            logger.info("Stopping feeds...")
            await self._manager.stop()
            
            # Final stats
            stats = self._manager.get_stats()
            logger.info(f"Final stats: {stats}")


def create_demo_configs() -> list[FeedConfig]:
    """Create demo configurations for testing."""
    return [
        FeedConfig(
            vendor=Vendor.DATABENTO,
            symbols=["ESZ4", "NQZ4", "CLZ4", "GCZ4"],
            api_key="demo-key",
            dataset="GLBX.MDP3",
            host="localhost",
            port=13000,
        ),
        FeedConfig(
            vendor=Vendor.BLOOMBERG,
            symbols=["ESZ4 Index", "NQZ4 Index"],
            host="localhost",
            port=8194,
        ),
        # CME direct is typically disabled unless you have multicast access
        # FeedConfig(
        #     vendor=Vendor.CME,
        #     symbols=["ESZ4", "NQZ4"],
        #     host="224.0.28.1",
        #     port=14310,
        #     enabled=False,
        # ),
    ]


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="High-Performance Market Data Feed Handler"
    )
    
    parser.add_argument(
        "--vendors",
        type=str,
        default="bloomberg",
        help="Comma-separated list of vendors (databento,bloomberg,cme)"
    )
    
    parser.add_argument(
        "--symbols",
        type=str,
        default="ESZ4,NQZ4",
        help="Comma-separated list of symbols"
    )
    
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run in demo mode with mock data"
    )
    
    parser.add_argument(
        "--aggregation-timeframe",
        type=int,
        default=60,
        help="OHLCV bar timeframe in seconds"
    )
    
    return parser.parse_args()


async def main() -> None:
    """Main entry point."""
    args = parse_args()
    
    if args.demo:
        logger.info("Loading demo")
        # Use demo configs
        configs = create_demo_configs()
        # Only enable Bloomberg for demo (it has mock data)
        for config in configs:
            if config.vendor != Vendor.BLOOMBERG:
                config.enabled = False
    else:
        # Parse vendor/symbol args
        vendors = [Vendor(v.strip()) for v in args.vendors.split(",")]
        symbols = [s.strip() for s in args.symbols.split(",")]
        
        configs = [
            FeedConfig(vendor=v, symbols=symbols)
            for v in vendors
        ]
    
    # Run application
    app = MarketDataApp()
    await app.run(
        configs=configs,
        enable_aggregation=True,
        aggregation_timeframe=args.aggregation_timeframe,
    )


if __name__ == "__main__":
    asyncio.run(main())

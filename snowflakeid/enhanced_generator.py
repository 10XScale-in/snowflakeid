"""
Enhanced Snowflake ID generator with distributed coordination and performance optimizations.

This module provides an enterprise-grade Snowflake ID generator suitable for
microservice deployments with distributed databases across multiple locations.
"""

import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

from .config import EnhancedSnowflakeIDConfig
from .distributed import (
    NodeRegistry,
    create_node_registry,
    HeartbeatManager,
    DistributedConfig,
)

logger = logging.getLogger(__name__)

# Constants from original generator
DEFAULT_EPOCH_MS = 1723323246031
BASE62_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
BASE62_BASE = len(BASE62_CHARS)


@dataclass(frozen=True)
class SnowflakeInfo:
    """Holds the extracted components of a Snowflake ID."""

    timestamp_ms: int  # Timestamp in milliseconds since the epoch
    readable_timestamp: str  # Human-readable timestamp string
    node_id: int
    worker_id: int
    sequence: int


class EnhancedSnowflakeGenerator:
    """
    Enhanced Snowflake ID generator with distributed coordination and performance optimizations.

    Features:
    - Distributed node/worker ID coordination
    - Batch ID generation for high throughput
    - Environment-aware configuration
    - Performance monitoring and health checks
    - Graceful shutdown with cleanup
    """

    def __init__(self, config: Optional[EnhancedSnowflakeIDConfig] = None):
        """
        Initialize the enhanced Snowflake ID generator.

        Args:
            config: Enhanced configuration. If None, loads from environment.
        """
        # Load configuration
        if config is None:
            from .config import ConfigurationLoader

            config = ConfigurationLoader.load_config()

        self.config = config

        # Core generation state (shared between sync and async)
        self.last_timestamp: int = -1
        self.sequence: int = 0

        # Locking for thread safety
        self.async_lock = asyncio.Lock()
        self.sync_lock = threading.Lock()

        # Distributed coordination components
        self.node_registry: Optional[NodeRegistry] = None
        self.heartbeat_manager: Optional[HeartbeatManager] = None
        self._coordination_initialized = False

        # Performance optimization components
        self.id_pool: Optional[asyncio.Queue] = None
        self.pool_refill_task: Optional[asyncio.Task] = None
        self._pool_enabled = config.enable_batch_generation

        # Monitoring and health
        self.generation_count = 0
        self.error_count = 0
        self.last_error_time = 0
        self._started_at = time.time()

        # State management
        self._initialized = False
        self._shutting_down = False

    async def initialize(self) -> bool:
        """
        Initialize the generator with distributed coordination.

        Returns:
            bool: True if initialization successful
        """
        if self._initialized:
            return True

        try:
            logger.info("Initializing enhanced Snowflake generator")

            # Initialize distributed coordination if enabled
            if self.config.auto_discover_node_id or self.config.auto_discover_worker_id:
                await self._initialize_distributed_coordination()

            # Initialize performance features
            if self._pool_enabled:
                await self._initialize_id_pool()

            self._initialized = True
            logger.info(
                f"Snowflake generator initialized with node_id={self.config.node_id}, worker_id={self.config.worker_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Snowflake generator: {e}")
            self.error_count += 1
            self.last_error_time = time.time()
            return False

    async def _initialize_distributed_coordination(self):
        """Initialize distributed coordination components."""
        if not self.config.distributed_config:
            raise ValueError("Distributed configuration required for auto-discovery")

        # Create node registry
        self.node_registry = create_node_registry(self.config.distributed_config)

        # Discover and register node ID if needed
        if self.config.auto_discover_node_id:
            try:
                discovered_node_id = (
                    await self.node_registry.discover_available_node_id()
                )
                # Update config with discovered node_id (need to create new config since it's frozen)
                config_dict = self.config.to_dict()
                config_dict["node_id"] = discovered_node_id
                object.__setattr__(self.config, "node_id", discovered_node_id)
                logger.info(f"Discovered node_id: {discovered_node_id}")
            except Exception as e:
                if self.config.fallback_strategy == "fail":
                    raise
                elif self.config.fallback_strategy == "random":
                    import random

                    max_node_id = (1 << self.config.node_bits) - 1
                    fallback_node_id = random.randint(0, max_node_id)
                    object.__setattr__(self.config, "node_id", fallback_node_id)
                    logger.warning(
                        f"Node discovery failed, using random node_id: {fallback_node_id}"
                    )
                # For "local_cache" strategy, keep the configured node_id

        # Discover and register worker ID if needed
        if self.config.auto_discover_worker_id:
            try:
                # For simplicity, use worker_id 0 unless already taken
                worker_id = 0
                max_worker_id = (1 << self.config.worker_bits) - 1

                for candidate_worker_id in range(max_worker_id + 1):
                    success = await self.node_registry.register_worker(
                        self.config.node_id, candidate_worker_id
                    )
                    if success:
                        worker_id = candidate_worker_id
                        break
                else:
                    raise RuntimeError("No available worker IDs")

                object.__setattr__(self.config, "worker_id", worker_id)
                logger.info(f"Registered worker_id: {worker_id}")

            except Exception as e:
                if self.config.fallback_strategy == "fail":
                    raise
                elif self.config.fallback_strategy == "random":
                    import random

                    max_worker_id = (1 << self.config.worker_bits) - 1
                    fallback_worker_id = random.randint(0, max_worker_id)
                    object.__setattr__(self.config, "worker_id", fallback_worker_id)
                    logger.warning(
                        f"Worker registration failed, using random worker_id: {fallback_worker_id}"
                    )

        # Start heartbeat manager
        if self.node_registry:
            self.heartbeat_manager = HeartbeatManager(
                self.node_registry, self.config.distributed_config
            )
            self.heartbeat_manager.start_heartbeat(
                self.config.node_id, self.config.worker_id
            )

        self._coordination_initialized = True

    async def _initialize_id_pool(self):
        """Initialize the ID pool for batch generation."""
        self.id_pool = asyncio.Queue(maxsize=self.config.pool_size)

        # Pre-fill the pool with some IDs
        initial_batch = await self._generate_batch_internal(self.config.batch_size)
        for snowflake_id in initial_batch:
            await self.id_pool.put(snowflake_id)

        # Start background refill task
        self.pool_refill_task = asyncio.create_task(self._pool_refill_loop())
        logger.info(f"ID pool initialized with {len(initial_batch)} IDs")

    async def _pool_refill_loop(self):
        """Background task to maintain the ID pool."""
        while not self._shutting_down:
            try:
                # Check if pool needs refilling
                if self.id_pool and self.id_pool.qsize() < self.config.batch_size:
                    batch = await self._generate_batch_internal(self.config.batch_size)

                    # Add IDs to pool (non-blocking)
                    for snowflake_id in batch:
                        try:
                            self.id_pool.put_nowait(snowflake_id)
                        except asyncio.QueueFull:
                            break  # Pool is full, stop adding

                await asyncio.sleep(0.01)  # Small delay to prevent busy waiting

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in pool refill loop: {e}")
                await asyncio.sleep(1)  # Longer delay on error

    async def generate(self) -> int:
        """
        Generate a unique Snowflake ID asynchronously.

        Returns:
            int: A unique Snowflake ID

        Raises:
            RuntimeError: If generator not initialized or clock moves backwards
            ValueError: If timestamp is before configured epoch
        """
        if not self._initialized:
            await self.initialize()

        # Try to get ID from pool first for better performance
        if self._pool_enabled and self.id_pool and not self.id_pool.empty():
            try:
                snowflake_id = self.id_pool.get_nowait()
                self.generation_count += 1
                return snowflake_id
            except asyncio.QueueEmpty:
                pass  # Fall back to direct generation

        # Generate ID directly
        async with self.async_lock:
            snowflake_id = await self._generate_single_id()
            self.generation_count += 1
            return snowflake_id

    async def generate_batch(self, count: int) -> List[int]:
        """
        Generate multiple unique Snowflake IDs efficiently.

        Args:
            count: Number of IDs to generate (max 10000)

        Returns:
            List[int]: List of unique Snowflake IDs
        """
        if not self._initialized:
            await self.initialize()

        if count <= 0:
            return []
        if count > 10000:
            raise ValueError("Batch size too large, maximum 10000 IDs per batch")

        async with self.async_lock:
            ids = await self._generate_batch_internal(count)
            self.generation_count += len(ids)
            return ids

    async def _generate_batch_internal(self, count: int) -> List[int]:
        """Internal batch generation (assumes lock is held)."""
        ids = []
        for _ in range(count):
            snowflake_id = await self._generate_single_id()
            ids.append(snowflake_id)
        return ids

    async def _generate_single_id(self) -> int:
        """Generate a single ID (assumes async lock is held)."""
        timestamp = self._get_timestamp()

        # Check for clock skew
        if timestamp < self.last_timestamp:
            raise RuntimeError(
                f"Clock moved backwards! Refusing to generate ID. "
                f"Last timestamp: {self.last_timestamp}, current timestamp: {timestamp}"
            )

        if timestamp == self.last_timestamp:
            # Increment sequence if within the same millisecond
            self.sequence = (self.sequence + 1) & ((1 << self.config.sequence_bits) - 1)
            if self.sequence == 0:
                # Sequence overflow, wait for the next millisecond
                timestamp = await self._wait_next_millis(self.last_timestamp)
        else:
            # Reset sequence for new millisecond
            self.sequence = 0

        self.last_timestamp = timestamp

        # Calculate time delta from epoch
        current_epoch = (
            self.config.epoch if self.config.epoch is not None else DEFAULT_EPOCH_MS
        )
        time_since_epoch = timestamp - current_epoch
        if time_since_epoch < 0:
            raise ValueError(
                f"Timestamp ({timestamp}) is before configured epoch ({current_epoch}). Cannot generate ID."
            )

        # Mask to ensure time_since_epoch fits into allocated bits
        time_shift = time_since_epoch & ((1 << self.config.time_bits) - 1)

        # Compose the ID from parts
        time_part = time_shift << (
            self.config.node_bits + self.config.worker_bits + self.config.sequence_bits
        )
        node_part = self.config.node_id << (
            self.config.worker_bits + self.config.sequence_bits
        )
        worker_part = self.config.worker_id << self.config.sequence_bits
        sequence_part = self.sequence

        return time_part | node_part | worker_part | sequence_part

    def generate_sync(self) -> int:
        """
        Generate a unique Snowflake ID synchronously.

        Returns:
            int: A unique Snowflake ID
        """
        with self.sync_lock:
            timestamp = self._get_timestamp()

            # Check for clock skew
            if timestamp < self.last_timestamp:
                raise RuntimeError(
                    f"Clock moved backwards! Refusing to generate ID. "
                    f"Last timestamp: {self.last_timestamp}, current timestamp: {timestamp}"
                )

            if timestamp == self.last_timestamp:
                # Increment sequence if within the same millisecond
                self.sequence = (self.sequence + 1) & (
                    (1 << self.config.sequence_bits) - 1
                )
                if self.sequence == 0:
                    # Sequence overflow, wait for the next millisecond
                    timestamp = self._wait_next_millis_sync(self.last_timestamp)
            else:
                # Reset sequence for new millisecond
                self.sequence = 0

            self.last_timestamp = timestamp

            # Calculate time delta from epoch
            current_epoch = (
                self.config.epoch if self.config.epoch is not None else DEFAULT_EPOCH_MS
            )
            time_since_epoch = timestamp - current_epoch
            if time_since_epoch < 0:
                raise ValueError(
                    f"Timestamp ({timestamp}) is before configured epoch ({current_epoch}). Cannot generate ID."
                )

            # Mask to ensure time_since_epoch fits into allocated bits
            time_shift = time_since_epoch & ((1 << self.config.time_bits) - 1)

            # Compose the ID from parts
            time_part = time_shift << (
                self.config.node_bits
                + self.config.worker_bits
                + self.config.sequence_bits
            )
            node_part = self.config.node_id << (
                self.config.worker_bits + self.config.sequence_bits
            )
            worker_part = self.config.worker_id << self.config.sequence_bits
            sequence_part = self.sequence

            self.generation_count += 1
            return time_part | node_part | worker_part | sequence_part

    def _get_timestamp(self) -> int:
        """Get the current timestamp in milliseconds."""
        return int(time.time() * 1000)

    async def _wait_next_millis(self, last_timestamp: int) -> int:
        """Asynchronously wait until the next millisecond."""
        timestamp = self._get_timestamp()
        while timestamp <= last_timestamp:
            await asyncio.sleep(0.001)  # Sleep for 1 millisecond
            timestamp = self._get_timestamp()
        return timestamp

    def _wait_next_millis_sync(self, last_timestamp: int) -> int:
        """Synchronously wait until the next millisecond."""
        timestamp = self._get_timestamp()
        while timestamp <= last_timestamp:
            time.sleep(0.0001)  # Sleep for 0.1 millisecond
            timestamp = self._get_timestamp()
        return timestamp

    @staticmethod
    def encode_base62(snowflake_id: int) -> str:
        """Encode a Snowflake ID into a Base62 string."""
        if snowflake_id == 0:
            return BASE62_CHARS[0]
        if snowflake_id < 0:
            raise ValueError(
                "Snowflake ID must be a non-negative integer for Base62 encoding."
            )

        encoded_chars = []
        while snowflake_id > 0:
            snowflake_id, remainder = divmod(snowflake_id, BASE62_BASE)
            encoded_chars.append(BASE62_CHARS[remainder])
        return "".join(reversed(encoded_chars))

    @staticmethod
    def decode_base62(encoded_id: str) -> int:
        """Decode a Base62 string into a Snowflake ID."""
        if not encoded_id:
            raise ValueError("Encoded ID string cannot be empty.")

        decoded_id = 0
        for char_val in encoded_id:
            try:
                decoded_id = decoded_id * BASE62_BASE + BASE62_CHARS.index(char_val)
            except ValueError:
                raise ValueError(
                    f"Invalid character '{char_val}' in Base62 encoded string. "
                    f"Only characters from '{BASE62_CHARS}' are allowed."
                )
        return decoded_id

    def extract_snowflake_info(self, snowflake_id: int) -> SnowflakeInfo:
        """
        Extract the components from a Snowflake ID.

        Args:
            snowflake_id: The Snowflake ID to parse

        Returns:
            SnowflakeInfo: Extracted components
        """
        if not isinstance(snowflake_id, int) or snowflake_id < 0:
            raise ValueError("Snowflake ID must be a non-negative integer.")

        # Define masks and shifts based on configuration
        sequence_bits = self.config.sequence_bits
        worker_bits = self.config.worker_bits
        node_bits = self.config.node_bits

        # Mask for sequence (lowest bits)
        sequence_mask = (1 << sequence_bits) - 1

        # Shifts required to isolate each part
        worker_shift = sequence_bits
        node_shift = sequence_bits + worker_bits
        time_shift_extract = sequence_bits + worker_bits + node_bits

        # Extract components using masks and bitwise right shifts
        sequence = snowflake_id & sequence_mask
        worker_id = (snowflake_id >> worker_shift) & ((1 << worker_bits) - 1)
        node_id = (snowflake_id >> node_shift) & ((1 << node_bits) - 1)
        timestamp_delta = (snowflake_id >> time_shift_extract) & (
            (1 << self.config.time_bits) - 1
        )

        # Reconstruct the full timestamp
        current_epoch = (
            self.config.epoch if self.config.epoch is not None else DEFAULT_EPOCH_MS
        )
        timestamp_ms = timestamp_delta + current_epoch

        # Format timestamp for readability
        readable_timestamp = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(timestamp_ms / 1000)
        )

        return SnowflakeInfo(
            timestamp_ms=timestamp_ms,
            readable_timestamp=readable_timestamp,
            node_id=node_id,
            worker_id=worker_id,
            sequence=sequence,
        )

    def get_stats(self) -> dict:
        """Get generator statistics."""
        uptime = time.time() - self._started_at
        return {
            "generation_count": self.generation_count,
            "error_count": self.error_count,
            "uptime_seconds": uptime,
            "generation_rate": self.generation_count / uptime if uptime > 0 else 0,
            "node_id": self.config.node_id,
            "worker_id": self.config.worker_id,
            "last_timestamp": self.last_timestamp,
            "current_sequence": self.sequence,
            "pool_size": self.id_pool.qsize() if self.id_pool else 0,
            "coordination_initialized": self._coordination_initialized,
        }

    async def health_check(self) -> dict:
        """Perform a health check of the generator."""
        try:
            # Test ID generation
            start_time = time.time()
            test_id = await self.generate()
            generation_time = time.time() - start_time

            # Test coordination backend if available
            coordination_healthy = True
            if self.heartbeat_manager and self.node_registry:
                try:
                    coordination_healthy = await self.node_registry.heartbeat(
                        self.config.node_id, self.config.worker_id
                    )
                except Exception:
                    coordination_healthy = False

            return {
                "healthy": True,
                "generation_time_ms": generation_time * 1000,
                "test_id": test_id,
                "coordination_healthy": coordination_healthy,
                "timestamp": datetime.utcnow().isoformat(),
                **self.get_stats(),
            }

        except Exception as e:
            return {
                "healthy": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.utcnow().isoformat(),
                **self.get_stats(),
            }

    async def shutdown(self, timeout: int = None) -> dict:
        """
        Gracefully shutdown the generator.

        Args:
            timeout: Shutdown timeout in seconds

        Returns:
            dict: Shutdown summary
        """
        timeout = timeout or self.config.graceful_shutdown_timeout
        self._shutting_down = True

        shutdown_info = {
            "final_generation_count": self.generation_count,
            "final_sequence": self.sequence,
            "last_timestamp": self.last_timestamp,
            "drained_pool_size": 0,
        }

        try:
            # Stop background tasks
            if self.pool_refill_task:
                self.pool_refill_task.cancel()
                try:
                    await asyncio.wait_for(self.pool_refill_task, timeout=5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

            # Stop heartbeat
            if self.heartbeat_manager:
                self.heartbeat_manager.stop_heartbeat()

            # Unregister from coordination backend
            if self.node_registry:
                try:
                    await asyncio.wait_for(
                        self.node_registry.unregister_node(self.config.node_id),
                        timeout=timeout,
                    )
                except asyncio.TimeoutError:
                    logger.warning("Timeout during node unregistration")

            # Drain ID pool
            if self.id_pool:
                drained_count = 0
                while not self.id_pool.empty():
                    try:
                        self.id_pool.get_nowait()
                        drained_count += 1
                    except asyncio.QueueEmpty:
                        break
                shutdown_info["drained_pool_size"] = drained_count

            logger.info(f"Snowflake generator shutdown complete: {shutdown_info}")
            return shutdown_info

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            shutdown_info["shutdown_error"] = str(e)
            return shutdown_info

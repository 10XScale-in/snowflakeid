import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

# Constants
DEFAULT_EPOCH_MS = 1723323246031  # Default epoch: 2024-08-12 20:54:06.031 UTC
BASE62_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
BASE62_BASE = len(BASE62_CHARS)


@dataclass(frozen=True)
class SnowflakeIDConfig:
    """
    Configuration for Snowflake ID generators.

    Defines the bit allocation for timestamp, node ID, worker ID, and sequence number,
    as well as the epoch and specific node/worker IDs.

    Attributes:
        epoch (Optional[int]): Custom epoch in milliseconds. If None, DEFAULT_EPOCH_MS is used.
        total_bits (int): Total bits for the ID (e.g., 64).
        time_bits (int): Number of bits allocated for the timestamp.
        node_bits (int): Number of bits allocated for the node ID.
        worker_bits (int): Number of bits allocated for the worker ID.
        node_id (int): The specific ID for this node. Must be within `0` to `(1 << node_bits) - 1`.
        worker_id (int): The specific ID for this worker. Must be within `0` to `(1 << worker_bits) - 1`.
        sequence_bits (Optional[int]): Number of bits for the sequence number.
                                       Calculated as `total_bits - time_bits - node_bits - worker_bits`.
    """
    epoch: Optional[int] = None
    total_bits: int = 64
    time_bits: int = 39
    node_bits: int = 7
    worker_bits: int = 5
    node_id: int = 0
    worker_id: int = 0
    sequence_bits: Optional[int] = None  # Calculated automatically

    def __post_init__(self):
        """
        Calculates sequence_bits and validates the configuration after initialization.
        """
        # Calculate sequence bits automatically based on other bit allocations.
        calculated_sequence_bits = self.total_bits - self.time_bits - self.node_bits - self.worker_bits
        object.__setattr__(self, 'sequence_bits', calculated_sequence_bits)
        # Validate configuration now that all fields, including sequence_bits, are set.
        self._validate_config()

    def _validate_config(self):
        """
        Validates the Snowflake ID configuration settings.

        Raises:
            ValueError: If any configuration setting is invalid (e.g., bit counts, ID ranges).
        """
        if not isinstance(self.time_bits, int) or self.time_bits <= 0:
            raise ValueError("time_bits must be a positive integer.")
        if not isinstance(self.node_bits, int) or self.node_bits <= 0:
            raise ValueError("node_bits must be a positive integer.")
        if not isinstance(self.worker_bits, int) or self.worker_bits <= 0:
            raise ValueError("worker_bits must be a positive integer.")
        if not isinstance(self.sequence_bits, int) or self.sequence_bits <= 0:
            raise ValueError(
                "sequence_bits must be a positive integer (derived from other bit allocations)."
            )

        # Check if node_id is within the allocated bits
        max_node_id = (1 << self.node_bits) - 1
        if not (0 <= self.node_id <= max_node_id):
            raise ValueError(
                f"Node ID ({self.node_id}) must be between 0 and {max_node_id} (inclusive)."
            )

        # Check if worker_id is within the allocated bits
        max_worker_id = (1 << self.worker_bits) - 1
        if not (0 <= self.worker_id <= max_worker_id):
            raise ValueError(
                f"Worker ID ({self.worker_id}) must be between 0 and {max_worker_id} (inclusive)."
            )

        # Validate that the sum of allocated bits equals total_bits
        expected_total_bits = (
            self.time_bits + self.node_bits + self.worker_bits + self.sequence_bits
        )
        if self.total_bits != expected_total_bits:
            raise ValueError(
                f"The sum of time_bits, node_bits, worker_bits, and sequence_bits ({expected_total_bits}) "
                f"must equal total_bits ({self.total_bits})." # Ensure spacing for long lines
            )

        # Validate epoch (if provided, otherwise DEFAULT_EPOCH_MS is used which is assumed valid)
        current_epoch_to_check = self.epoch if self.epoch is not None else DEFAULT_EPOCH_MS
        if not isinstance(current_epoch_to_check, int) or current_epoch_to_check <= 0:
            raise ValueError("Epoch must be a positive integer representing milliseconds.")


@dataclass(frozen=True)
class SnowflakeInfo:
    """Holds the extracted components of a Snowflake ID."""
    timestamp_ms: int  # Timestamp in milliseconds since the epoch
    readable_timestamp: str  # Human-readable timestamp string
    node_id: int
    worker_id: int
    sequence: int


class SnowflakeGenerator:
    """
    Snowflake ID generator supporting both asynchronous and synchronous generation.

    Generates unique, time-ordered IDs. It uses separate locks for async and sync
    operations to ensure thread-safety and async-safety. The `last_timestamp`
    and `sequence` are shared within an instance to ensure uniqueness across
    all calls to that instance.

    Attributes:
        config (SnowflakeIDConfig): Configuration for the generator.
        last_timestamp (int): The last timestamp (in ms) at which an ID was generated.
                              Shared between sync and async generation methods.
        sequence (int): The sequence number for the current millisecond.
                        Shared between sync and async generation methods.
        async_lock (asyncio.Lock): Lock for asynchronous ID generation.
        sync_lock (threading.Lock): Lock for synchronous ID generation.
    """

    def __init__(self, config: Optional[SnowflakeIDConfig] = None):
        """
        Initializes the Snowflake ID generator.

        Args:
            config (Optional[SnowflakeIDConfig]): Configuration for the generator.
                                                 If None, default configuration is used.
        """
        self.config = config or SnowflakeIDConfig()
        self.last_timestamp: int = -1
        self.sequence: int = 0
        self.async_lock = asyncio.Lock() # Renamed lock to async_lock
        self.sync_lock = threading.Lock()  # Added sync_lock

    async def generate(self) -> int: # This is the async generate
        """
        Generates a unique Snowflake ID asynchronously.

        Returns:
            int: A unique Snowflake ID.

        Raises:
            RuntimeError: If the clock moves backwards.
            ValueError: If the timestamp is before the configured epoch.
        """
        async with self.async_lock:
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
            current_epoch = self.config.epoch if self.config.epoch is not None else DEFAULT_EPOCH_MS
            time_since_epoch = timestamp - current_epoch
            if time_since_epoch < 0:
                raise ValueError(
                    f"Timestamp ({timestamp}) is before configured epoch ({current_epoch}). Cannot generate ID."
                )

            # Mask to ensure time_since_epoch fits into allocated bits
            time_shift = time_since_epoch & ((1 << self.config.time_bits) - 1)

            # Compose the ID from parts
            # Shift timestamp to the left by the sum of node, worker, and sequence bits
            time_part = time_shift << (
                self.config.node_bits + self.config.worker_bits + self.config.sequence_bits
            )
            # Shift node ID to the left by the sum of worker and sequence bits
            node_part = self.config.node_id << (
                self.config.worker_bits + self.config.sequence_bits
            )
            # Shift worker ID to the left by sequence bits
            worker_part = self.config.worker_id << self.config.sequence_bits
            # Sequence part is already in the lowest bits
            sequence_part = self.sequence

            final_bits = time_part | node_part | worker_part | sequence_part
            return final_bits

    def _get_timestamp(self) -> int:
        """
        Gets the current timestamp in milliseconds.

        Returns:
            int: Current timestamp in milliseconds.
        """
        return int(time.time() * 1000)

    async def _wait_next_millis(self, last_timestamp: int) -> int:
        """
        Asynchronously waits until the next millisecond.

        Args:
            last_timestamp (int): The timestamp of the last ID generation.

        Returns:
            int: The new timestamp after waiting.
        """
        timestamp = self._get_timestamp()
        while timestamp <= last_timestamp:
            await asyncio.sleep(0.001)  # Sleep for 1 millisecond
            timestamp = self._get_timestamp()
        return timestamp

    @staticmethod
    def encode_base62(snowflake_id: int) -> str:
        """
        Encodes a Snowflake ID (integer) into a Base62 string.

        Args:
            snowflake_id (int): The Snowflake ID to encode.

        Returns:
            str: The Base62 encoded string representation of the Snowflake ID.
        """
        if snowflake_id == 0:
            return BASE62_CHARS[0]
        if snowflake_id < 0:
            raise ValueError("Snowflake ID must be a non-negative integer for Base62 encoding.")

        encoded_chars = []
        while snowflake_id > 0:
            snowflake_id, remainder = divmod(snowflake_id, BASE62_BASE)
            encoded_chars.append(BASE62_CHARS[remainder])
        return "".join(reversed(encoded_chars))

    @staticmethod
    def decode_base62(encoded_id: str) -> int:
        """
        Decodes a Base62 string into a Snowflake ID (integer).

        Args:
            encoded_id (str): The Base62 encoded string.

        Returns:
            int: The decoded Snowflake ID.

        Raises:
            ValueError: If the encoded_id contains characters not in BASE62_CHARS or is empty.
        """
        if not encoded_id:
            raise ValueError("Encoded ID string cannot be empty.")

        decoded_id = 0
        for char_val in encoded_id:
            try:
                # Efficiently build the number by multiplying by base and adding new value
                decoded_id = decoded_id * BASE62_BASE + BASE62_CHARS.index(char_val)
            except ValueError:
                raise ValueError(
                    f"Invalid character '{char_val}' in Base62 encoded string. "
                    f"Only characters from '{BASE62_CHARS}' are allowed."
                )
        return decoded_id

    def extract_snowflake_info(self, snowflake_id: int) -> SnowflakeInfo: # Updated return type hint
        """
        Extracts the components (timestamp, node ID, worker ID, sequence) from a Snowflake ID.

        The method uses the bit allocation defined in `self.config`.

        Args:
            snowflake_id (int): The Snowflake ID to parse.

        Returns:
            SnowflakeInfo: An object containing the extracted components:
                           - timestamp_ms (int): Timestamp in milliseconds since the Unix epoch.
                           - readable_timestamp (str): Human-readable timestamp (YYYY-MM-DD HH:MM:SS).
                           - node_id (int): Extracted node ID.
                           - worker_id (int): Extracted worker ID.
                           - sequence (int): Extracted sequence number.
        
        Raises:
            ValueError: If snowflake_id is not a non-negative integer.
        """
        if not isinstance(snowflake_id, int) or snowflake_id < 0:
            raise ValueError("Snowflake ID must be a non-negative integer.")

        # Define masks and shifts based on configuration for clarity
        sequence_bits = self.config.sequence_bits
        worker_bits = self.config.worker_bits
        node_bits = self.config.node_bits
        time_bits = self.config.time_bits

        # Mask for sequence (lowest bits)
        sequence_mask = (1 << sequence_bits) - 1

        # Shifts required to isolate each part
        worker_shift = sequence_bits
        node_shift = sequence_bits + worker_bits
        time_shift_extract = sequence_bits + worker_bits + node_bits # Renamed for clarity

        # Extract components using masks and bitwise right shifts
        sequence = snowflake_id & sequence_mask
        worker_id = (snowflake_id >> worker_shift) & ((1 << worker_bits) - 1)
        node_id = (snowflake_id >> node_shift) & ((1 << node_bits) - 1)
        timestamp_delta = (snowflake_id >> time_shift_extract) & ((1 << time_bits) - 1)

        # Reconstruct the full timestamp
        current_epoch = self.config.epoch if self.config.epoch is not None else DEFAULT_EPOCH_MS
        timestamp_ms = timestamp_delta + current_epoch
        # Format timestamp for readability
        # Using time.gmtime for UTC representation if desired, or localtime for local time.
        # The original used localtime. For consistency with epochs usually being UTC, gmtime might be better
        # but sticking to localtime to avoid breaking change in output format unless specified.
        readable_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp_ms / 1000))
        # Optional: Add milliseconds part to the readable string for more precision
        # ms_part = int(timestamp_ms % 1000)
        # readable_timestamp += f".{ms_part:03d}"

        return SnowflakeInfo(
            timestamp_ms=timestamp_ms,
            readable_timestamp=readable_timestamp,
            node_id=node_id,
            worker_id=worker_id,
            sequence=sequence
        )

    # --- Synchronous methods ---
    def _wait_next_millis_sync(self, last_timestamp: int) -> int:
        """
        Synchronously waits until the next millisecond.

        Args:
            last_timestamp (int): The timestamp of the last ID generation.

        Returns:
            int: The new timestamp after waiting.
        """
        timestamp = self._get_timestamp()
        while timestamp <= last_timestamp:
            time.sleep(0.0001)  # Sleep for a short duration (e.g., 0.1 ms)
            timestamp = self._get_timestamp()
        return timestamp

    def generate_sync(self) -> int:
        """
        Generates a unique Snowflake ID synchronously.

        Returns:
            int: A unique Snowflake ID.

        Raises:
            RuntimeError: If the clock moves backwards.
            ValueError: If the timestamp is before the configured epoch.
        """
        with self.sync_lock: # Use sync_lock
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
                    timestamp = self._wait_next_millis_sync(self.last_timestamp)
            else:
                # Reset sequence for new millisecond
                self.sequence = 0

            self.last_timestamp = timestamp

            # Calculate time delta from epoch
            current_epoch = self.config.epoch if self.config.epoch is not None else DEFAULT_EPOCH_MS
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

            final_bits = time_part | node_part | worker_part | sequence_part
            return final_bits

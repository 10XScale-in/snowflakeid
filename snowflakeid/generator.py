import asyncio
import time
from dataclasses import dataclass
import datetime as dt
from typing import Optional, Dict

# Constants
DEFAULT_EPOCH = 1735689600000
BASE62_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
BASE62_BASE = len(BASE62_CHARS)


@dataclass(frozen=True)
class SnowflakeIDInfo:
    datetime: dt.datetime
    node_id: int
    worker_id: int
    sequence: int



@dataclass(frozen=True)
class SnowflakeIDConfig:
    """Configuration for the Snowflake ID generator."""
    epoch: int = DEFAULT_EPOCH
    total_bits: int = 64
    time_bits: int = 39
    node_bits: int = 7
    worker_bits: int = 5
    node_id: int = 0
    worker_id: int = 0
    sequence_bits: int = None  # Calculated automatically below

    def __post_init__(self):
        # Calculate sequence bits automatically
        object.__setattr__(self, 'sequence_bits', self.total_bits - self.time_bits - self.node_bits - self.worker_bits)
        # Validate configuration now that all fields are set
        self._validate_config()

    def _validate_config(self):
        """Validate the configuration settings."""
        # check node id is within bounds based on the number of bits
        max_node_id = (1 << self.node_bits) - 1
        if self.node_id > max_node_id:
            raise ValueError(f"Node ID ({self.node_id}) must be less than or equal to {max_node_id}")

        # check worker id is within bounds based on the number of bits
        max_worker_id = (1 << self.worker_bits) - 1
        if self.worker_id > max_worker_id:
            raise ValueError(f"Worker ID ({self.worker_id}) must be less than or equal to {max_worker_id}")

        # validate total bits
        if self.total_bits <= sum([self.time_bits, self.node_bits, self.worker_bits, 1]):
            raise ValueError("The sum of time bits, node bits, worker bits must equal total bits")


class SnowflakeIDGenerator:
    """Asynchronous Snowflake ID generator."""

    def __init__(self, config: Optional[SnowflakeIDConfig] = None):
        self.config = config or SnowflakeIDConfig()
        self.last_timestamp = -1
        self.sequence = 0
        self.lock = asyncio.Lock()

    async def generate(self) -> int:
        """Generate a unique Snowflake ID."""
        async with self.lock:
            timestamp = self._get_timestamp()
            if timestamp < self.last_timestamp:
                raise RuntimeError("Clock moved backwards! Refusing to generate IDs.")
            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & ((1 << self.config.sequence_bits) - 1)
                if self.sequence == 0:
                    timestamp = await self._wait_next_millis(self.last_timestamp)
            else:
                self.sequence = 0

            self.last_timestamp = timestamp
            time_since_epoch = timestamp - self.config.epoch
            time_shift = time_since_epoch & ((1 << self.config.time_bits) - 1)

            # Calculate the final Snowflake ID
            time_part = time_shift << (self.config.node_bits + self.config.worker_bits + self.config.sequence_bits)
            node_part = self.config.node_id << (self.config.worker_bits + self.config.sequence_bits)
            worker_part = self.config.worker_id << self.config.sequence_bits
            sequence_part = self.sequence
            final_bits = time_part | node_part | worker_part | sequence_part

            return final_bits

    def _get_timestamp(self) -> int:
        """Get the current timestamp in milliseconds."""
        return int(time.time() * 1000)

    async def _wait_next_millis(self, last_timestamp: int) -> int:
        """Wait until the next millisecond."""
        while (timestamp := self._get_timestamp()) <= last_timestamp:
            await asyncio.sleep(0.001)
        return timestamp

    @staticmethod
    def encode_base62(snowflake_id: int) -> str:
        """Encodes a Snowflake ID to a Base62 string."""
        if snowflake_id == 0:
            return BASE62_CHARS[0]

        encoded = ""
        while snowflake_id > 0:
            snowflake_id, remainder = divmod(snowflake_id, BASE62_BASE)
            encoded = BASE62_CHARS[remainder] + encoded
        return encoded

    @staticmethod
    def decode_base62(encoded_id: str) -> int:
        """Decodes a Base62 string to a Snowflake ID."""
        decoded = 0
        for i, char in enumerate(reversed(encoded_id)):
            decoded += BASE62_CHARS.index(char) * (BASE62_BASE ** i)
        return decoded

    def extract_snowflake_info(self, snowflake_id: int) -> SnowflakeIDInfo:
        """Extracts the components of a Snowflake ID.

        Returns:
            A dictionary containing the timestamp, worker ID, node ID, and sequence number.
        """

        sequence_mask = (1 << self.config.sequence_bits) - 1
        worker_mask = ((1 << self.config.worker_bits) - 1) << self.config.sequence_bits
        node_mask = ((1 << self.config.node_bits) - 1) << (self.config.worker_bits + self.config.sequence_bits)
        time_mask = ((1 << self.config.time_bits) - 1) << (
                self.config.node_bits + self.config.worker_bits + self.config.sequence_bits
        )

        timestamp = ((snowflake_id & time_mask) >> (
                self.config.node_bits + self.config.worker_bits + self.config.sequence_bits
        ))

        # **CORRECTED LINE:** Add epoch BEFORE shifting
        timestamp += self.config.epoch

        node_id = (snowflake_id & node_mask) >> (self.config.worker_bits + self.config.sequence_bits)
        worker_id = (snowflake_id & worker_mask) >> self.config.sequence_bits
        sequence = snowflake_id & sequence_mask

        # parse timestamp to readable format
        # timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))
        return SnowflakeIDInfo(
            datetime=dt.datetime.fromtimestamp(timestamp / 1000),
            worker_id=worker_id,
            node_id=node_id,
            sequence=sequence
        )
        # return {
        #     "timestamp": timestamp,
        #     "worker_id": worker_id,
        #     "node_id": node_id,
        #     "sequence": sequence,
        # }

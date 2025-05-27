import asyncio
import time
from typing import List, Any

import pytest

from snowflakeid import (
    SnowflakeGenerator, # Updated class name
    SnowflakeIDConfig,
    SnowflakeInfo # Added SnowflakeInfo
)

# Define 48-bit configuration for testing
# total_bits = 48
# time_bits = 28 (allows for ~8.5 years of ms from epoch)
# node_bits = 5  (max 31)
# worker_bits = 5 (max 31)
# sequence_bits = 48 - 28 - 5 - 5 = 10 (1024 sequences per ms)
TEST_CONFIG_48BIT = SnowflakeIDConfig(
    total_bits=48,
    epoch=DEFAULT_EPOCH_MS, # Using the default epoch from the library
    time_bits=28,
    node_bits=5,
    worker_bits=5,
    node_id=10,  # Example node_id (0-31)
    worker_id=20 # Example worker_id (0-31)
    # sequence_bits is calculated automatically
)


# Helper Functions for Async Testing
async def generate_ids_concurrently(generator: SnowflakeGenerator, count: int) -> List[int]: # Updated type hint
    """Generates multiple Snowflake IDs concurrently using asyncio.gather."""
    tasks = [generator.generate() for _ in range(count)]
    return await asyncio.gather(*tasks)


# Helper function for Sync Testing
def generate_sync_ids(generator: SnowflakeGenerator, count: int) -> List[int]: # Updated type hint
    """Generates multiple Snowflake IDs synchronously."""
    return [generator.generate_sync() for _ in range(count)] # Updated method call


# Async Tests for 48-bit configuration

@pytest.mark.asyncio
async def test_async_snowflake_id_generation_48bit():
    """Test the generation of 48-bit Snowflake IDs asynchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT) # Updated class name
    snowflake_id = await generator.generate()

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    assert snowflake_id < (1 << 48), "Generated ID should be less than 2^48."
    assert snowflake_id.bit_length() <= 48, "Generated ID bit length should not exceed 48 bits."


@pytest.mark.asyncio
async def test_async_uniqueness_48bit():
    """Test uniqueness of 48-bit Snowflake IDs generated asynchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT) # Updated class name
    ids_count = 500
    ids = await generate_ids_concurrently(generator, ids_count)
    assert len(ids) == ids_count, f"Expected {ids_count} IDs, got {len(ids)}."
    assert len(set(ids)) == ids_count, "Generated asynchronous IDs should be unique."


@pytest.mark.asyncio
async def test_async_extract_snowflake_info_48bit():
    """Test extracting information from a 48-bit Snowflake ID generated asynchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT) # Updated class name
    
    current_time_ms = int(time.time() * 1000)
    snowflake_id = await generator.generate()
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    assert info.timestamp_ms >= current_time_ms - 50, "Extracted timestamp_ms should be around current time at start of test." # Attribute access
    assert info.timestamp_ms < current_time_ms + 500, "Extracted timestamp_ms is too far in the future." # Attribute access
    assert info.worker_id == TEST_CONFIG_48BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_48BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access


# Synchronous Tests for 48-bit configuration

def test_sync_snowflake_id_generation_48bit():
    """Test the generation of 48-bit Snowflake IDs synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT) # Updated class name
    snowflake_id = generator.generate_sync() # Updated method call

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    assert snowflake_id < (1 << 48), "Generated ID should be less than 2^48."
    assert snowflake_id.bit_length() <= 48, "Generated ID bit length should not exceed 48 bits."


def test_sync_uniqueness_48bit():
    """Test uniqueness of 48-bit Snowflake IDs generated synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT) # Updated class name
    ids_count = 500
    ids = generate_sync_ids(generator, ids_count)
    assert len(ids) == ids_count, f"Expected {ids_count} IDs, got {len(ids)}."
    assert len(set(ids)) == ids_count, "Generated synchronous IDs should be unique."


def test_sync_extract_snowflake_info_48bit():
    """Test extracting information from a 48-bit Snowflake ID generated synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT) # Updated class name
    
    current_time_ms = int(time.time() * 1000)
    snowflake_id = generator.generate_sync() # Updated method call
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    assert info.timestamp_ms >= current_time_ms, "Extracted timestamp_ms should be >= current time at start of test." # Attribute access
    assert info.timestamp_ms < current_time_ms + 500, "Extracted timestamp_ms is too far in the future." # Attribute access
    assert info.worker_id == TEST_CONFIG_48BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_48BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access

# Example of checking bit allocation if needed
def test_48bit_config_details():
    """Verify the calculated sequence bits for the 48-bit config."""
    assert TEST_CONFIG_48BIT.sequence_bits == 10, "Sequence bits should be 10 for this 48-bit config."
    assert TEST_CONFIG_48BIT.time_bits == 28
    assert TEST_CONFIG_48BIT.node_bits == 5
    assert TEST_CONFIG_48BIT.worker_bits == 5
    assert (TEST_CONFIG_48BIT.time_bits +
            TEST_CONFIG_48BIT.node_bits +
            TEST_CONFIG_48BIT.worker_bits +
            TEST_CONFIG_48BIT.sequence_bits) == 48

# Test with edge node/worker IDs for 48-bit
TEST_CONFIG_48BIT_EDGE_IDS = SnowflakeIDConfig(
    total_bits=48,
    epoch=DEFAULT_EPOCH_MS,
    time_bits=28,
    node_bits=5, # max node_id = 31
    worker_bits=5, # max worker_id = 31
    node_id=31,
    worker_id=0
    # sequence_bits = 10
)

def test_sync_extract_snowflake_info_48bit_edge_ids():
    """Test extracting info for 48-bit IDs with edge node/worker IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT_EDGE_IDS) # Updated class name
    snowflake_id = generator.generate_sync() # Updated method call
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info
    assert info.node_id == 31, "Incorrect node ID extracted for edge case." # Attribute access
    assert info.worker_id == 0, "Incorrect worker ID extracted for edge case." # Attribute access

@pytest.mark.asyncio
async def test_async_extract_snowflake_info_48bit_edge_ids():
    """Test extracting info for 48-bit IDs with edge node/worker IDs (async)."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT_EDGE_IDS) # Updated class name
    snowflake_id = await generator.generate()
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info
    assert info.node_id == 31, "Incorrect node ID extracted for edge case (async)." # Attribute access
    assert info.worker_id == 0, "Incorrect worker ID extracted for edge case (async)." # Attribute access

# Test with minimum sequence bits (e.g. 1 if other bits are maximized for 48-total)
# time_bits=28, node_bits=9, worker_bits=10 => 28+9+10 = 47. sequence_bits = 1
TEST_CONFIG_48BIT_MIN_SEQ = SnowflakeIDConfig(
    total_bits=48,
    epoch=DEFAULT_EPOCH_MS,
    time_bits=28, 
    node_bits=9, # max node_id = 511
    worker_bits=10, # max worker_id = 1023
    node_id=1,
    worker_id=1
    # sequence_bits = 48 - 28 - 9 - 10 = 1
)

def test_48bit_min_seq_config_details():
    """Verify sequence bits for min sequence config."""
    assert TEST_CONFIG_48BIT_MIN_SEQ.sequence_bits == 1

@pytest.mark.asyncio
async def test_async_uniqueness_48bit_min_seq():
    """Test uniqueness with minimal sequence bits (forces more timestamp waits)."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT_MIN_SEQ) # Updated class name
    ids_count = 10 
    ids = await generate_ids_concurrently(generator, ids_count)
    assert len(set(ids)) == ids_count, "Generated IDs should be unique even with minimal sequence bits."

    infos = [generator.extract_snowflake_info(id_val) for id_val in ids]
    timestamps = [info.timestamp_ms for info in infos] # Attribute access
    assert len(set(timestamps)) > 1, "Timestamps should advance due to sequence exhaustion."
    assert len(set(timestamps)) >= ids_count // (1 << TEST_CONFIG_48BIT_MIN_SEQ.sequence_bits), \
        "Timestamps should advance sufficiently."

def test_sync_uniqueness_48bit_min_seq():
    """Test sync uniqueness with minimal sequence bits."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT_MIN_SEQ) # Updated class name
    ids_count = 10
    ids = generate_sync_ids(generator, ids_count)
    assert len(set(ids)) == ids_count, "Generated sync IDs should be unique with minimal sequence bits."

    infos = [generator.extract_snowflake_info(id_val) for id_val in ids]
    timestamps = [info.timestamp_ms for info in infos] # Attribute access
    assert len(set(timestamps)) >= ids_count // (1 << TEST_CONFIG_48BIT_MIN_SEQ.sequence_bits), \
        "Timestamps should advance sufficiently for sync generator."

# Test with maximum sequence bits
# time_bits=10, node_bits=1, worker_bits=1 => 10+1+1 = 12. sequence_bits = 48-12 = 36
TEST_CONFIG_48BIT_MAX_SEQ = SnowflakeIDConfig(
    total_bits=48,
    epoch=DEFAULT_EPOCH_MS,
    time_bits=10, # Minimal time bits
    node_bits=1, 
    worker_bits=1, 
    node_id=0,
    worker_id=0
    # sequence_bits = 48 - 10 - 1 - 1 = 36
)

def test_48bit_max_seq_config_details():
    """Verify sequence bits for max sequence config."""
    assert TEST_CONFIG_48BIT_MAX_SEQ.sequence_bits == 36

@pytest.mark.asyncio
async def test_async_uniqueness_48bit_max_seq():
    """Test uniqueness with maximal sequence bits (less likely to need timestamp waits)."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT_MAX_SEQ) # Updated class name
    ids_count = 1000
    ids = await generate_ids_concurrently(generator, ids_count)
    assert len(set(ids)) == ids_count, "Generated IDs should be unique with maximal sequence bits."

    infos = [generator.extract_snowflake_info(id_val) for id_val in ids]
    timestamps = [info.timestamp_ms for info in infos] # Attribute access
    assert len(set(timestamps)) <= max(1, ids_count // 100) , "Timestamps should not advance much with many sequence bits."
Okay, `tests/test_48_bits.py` has been created with a comprehensive set of tests, including standard async/sync tests, tests for edge node/worker IDs, and tests for configurations with minimal and maximal sequence bits. This provides good coverage for the 48-bit scenario.

Now, I'll proceed to **Step 4: Create `tests/test_96_bits.py`**.

**Plan for `tests/test_96_bits.py`:**

1.  **Create the file `tests/test_96_bits.py`**.
2.  **Add necessary imports**: `asyncio`, `time`, `List`, `pytest`, `SnowflakeIDGenerator`, `SyncSnowflakeIDGenerator`, `SnowflakeIDConfig`, `DEFAULT_EPOCH_MS`.
3.  **Define `TEST_CONFIG_96BIT`**:
    *   `total_bits=96`
    *   `epoch=DEFAULT_EPOCH_MS`
    *   `time_bits=50` (Allows for billions of years, more than enough. Or adjust for a more "realistic" time range if desired, but 50 is fine for testing large total_bits)
    *   `node_bits=10` (max 1023)
    *   `worker_bits=10` (max 1023)
    *   `sequence_bits` will be calculated as `96 - 50 - 10 - 10 = 26` (approx 67 million sequences per ms).
    *   `node_id=50`
    *   `worker_id=75`
4.  **Add helper functions**: `generate_ids_concurrently` and `generate_sync_ids`.
5.  **Implement Async Tests (`SnowflakeIDGenerator`)**:
    *   `test_async_snowflake_id_generation_96bit`
    *   `test_async_uniqueness_96bit`
    *   `test_async_extract_snowflake_info_96bit`
6.  **Implement Sync Tests (`SyncSnowflakeIDGenerator`)**:
    *   `test_sync_snowflake_id_generation_96bit`
    *   `test_sync_uniqueness_96bit`
    *   `test_sync_extract_snowflake_info_96bit`
7.  **Add a few extra focused tests** similar to `test_48_bits.py` for config validation and edge conditions if time/complexity permits, but the core 6 tests are the priority.

I will construct the full content for this new file.

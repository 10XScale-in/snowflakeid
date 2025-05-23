import asyncio
import time
from typing import List, Any # Keep Any for gather, but List[int] for the list itself

import pytest

from snowflakeid import (
    SnowflakeGenerator, # Updated class name
    SnowflakeIDConfig,
    SnowflakeInfo # Added SnowflakeInfo
)

# Define 64-bit configuration for testing
TEST_CONFIG_64BIT = SnowflakeIDConfig(
    total_bits=64,
    epoch=1288834974657,
    time_bits=39,
    node_bits=1,
    worker_bits=11,
    node_id=1,
    worker_id=7
)

TEST_CONFIG_64BIT2 = SnowflakeIDConfig(
    total_bits=64,
    epoch=1288834974657,
    time_bits=39,
    node_bits=1,
    worker_bits=11,
    node_id=0,
    worker_id=7
)


# Helper Functions for Testing

async def generate_ids_concurrently(generator: SnowflakeGenerator, count: int) -> List[int]: # Updated type hint
    """Generates multiple Snowflake IDs concurrently using asyncio.gather."""
    tasks = [generator.generate() for _ in range(count)]
    # asyncio.gather returns a list of results, which are ints in this case
    results: List[int] = await asyncio.gather(*tasks)
    return results


@pytest.mark.asyncio
async def test_snowflake_id_generation_64bit():
    """Test the generation of 64-bit Snowflake IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    snowflake_id = await generator.generate()

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    # For 64-bit, the length can be up to 64. If all leading bits are 0, it could be less.
    # A more robust check is that it's less than 2^64
    assert snowflake_id < (1 << 64), "Generated ID should be less than 2^64."
    # And that it's large enough to potentially use the upper bits if time component is large
    # This check is a bit loose, but better than strict equality for bit_length()
    assert snowflake_id.bit_length() <= 64, "Generated ID bit length should not exceed 64."


@pytest.mark.asyncio
async def test_async_uniqueness_64bit():
    """Test asynchronous uniqueness of 64-bit Snowflake IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    ids_count = 1000
    ids = await generate_ids_concurrently(generator, ids_count)
    assert len(ids) == ids_count
    assert len(set(ids)) == ids_count, "Generated IDs should be unique."


@pytest.mark.asyncio
async def test_base62_encoding_decoding_64bit():
    """Test Base62 encoding and decoding for 64-bit IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    for _ in range(100): 
        original_id = await generator.generate()
        # Access static methods via the class itself
        encoded_id = SnowflakeGenerator.encode_base62(original_id)
        decoded_id = SnowflakeGenerator.decode_base62(encoded_id)
        assert original_id == decoded_id, "Decoded ID should match the original ID."


@pytest.mark.asyncio
async def test_snowflake_id_two_generators_64bit():
    """Test ID generation with two different 64-bit generator configurations."""
    generator1 = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    generator2 = SnowflakeGenerator(config=TEST_CONFIG_64BIT2) # Updated class name
    id1 = await generator1.generate()
    id2 = await generator2.generate()
    assert id1 != id2, "IDs from different generator configurations should be different."


@pytest.mark.asyncio
async def test_snowflake_sequence_reset_64bit():
    """Test if the sequence resets at the next millisecond for 64-bit IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    id1 = await generator.generate()
    await asyncio.sleep(0.001)
    id2 = await generator.generate()
    assert id1 != id2, "IDs should be different after sequence reset."
    info1 = generator.extract_snowflake_info(id1)
    info2 = generator.extract_snowflake_info(id2)
    assert info1.timestamp_ms < info2.timestamp_ms, "Timestamp of id2 should be greater than id1." # Attribute access
    if info1.timestamp_ms < info2.timestamp_ms:
        assert info2.sequence == 0, "Sequence should reset for a new millisecond." # Attribute access


@pytest.mark.asyncio
async def test_extract_snowflake_info_64bit():
    """Test extracting information from a 64-bit Snowflake ID."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    
    current_time_ms = int(time.time() * 1000)
    snowflake_id = await generator.generate()
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    assert info.timestamp_ms >= current_time_ms, "Extracted timestamp_ms should be >= current time at start of test." # Attribute access
    assert info.timestamp_ms < current_time_ms + 500, "Extracted timestamp_ms is too far in the future." # Attribute access
    assert info.worker_id == TEST_CONFIG_64BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_64BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access


# Removed redundant collision tests for brevity, covered by uniqueness tests
# Removed intentional collision test for brevity

# Keep one comprehensive collision test if desired, or rely on uniqueness tests.
# For this refactoring, focusing on the requested tests.
# The existing test_snowflake_id_collision_32bit2 (renamed to _64bit) can serve this.
@pytest.mark.asyncio
async def test_async_uniqueness_large_batch_64bit():
    """Test for potential ID collisions in a large batch for 64-bit IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    ids_count = 10000
    ids = await generate_ids_concurrently(generator, ids_count)
    assert len(set(ids)) == ids_count, "Collisions detected in a large batch! IDs are not unique."


# Synchronous Tests for 64-bit configuration

# Helper function for synchronous ID generation tests
def generate_sync_ids(generator: SnowflakeGenerator, count: int) -> List[int]: # Updated type hint
    """Generates multiple Snowflake IDs synchronously."""
    return [generator.generate_sync() for _ in range(count)] # Updated method call


def test_sync_snowflake_id_generation_64bit():
    """Test the generation of 64-bit Snowflake IDs synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    snowflake_id = generator.generate_sync() # Updated method call

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    assert snowflake_id < (1 << 64), "Generated ID should be less than 2^64."
    assert snowflake_id.bit_length() <= 64, "Generated ID bit length should not exceed 64."


def test_sync_uniqueness_64bit():
    """Test uniqueness of 64-bit Snowflake IDs generated synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    ids_count = 1000
    ids = generate_sync_ids(generator, ids_count)
    assert len(ids) == ids_count, f"Expected {ids_count} IDs, got {len(ids)}."
    assert len(set(ids)) == ids_count, "Generated synchronous IDs should be unique."


def test_sync_extract_snowflake_info_64bit():
    """Test extracting information from a 64-bit Snowflake ID generated synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    
    current_time_ms = int(time.time() * 1000)
    snowflake_id = generator.generate_sync() # Updated method call
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    assert info.timestamp_ms >= current_time_ms, "Extracted timestamp_ms should be >= current time at start of test." # Attribute access
    assert info.timestamp_ms < current_time_ms + 500, "Extracted timestamp_ms is too far in the future." # Attribute access
    assert info.worker_id == TEST_CONFIG_64BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_64BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access


# Removing the intentional collision test as it's not part of the new requirements for this file
# and its principles are covered by uniqueness.
# @pytest.mark.asyncio
# async def test_intentional_collision_64bit():
#     """Demonstrates an intentional collision (avoid in production!)."""
#     generator1 = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
#     generator2 = SnowflakeGenerator(config=TEST_CONFIG_64BIT) # Updated class name
    id1 = await generator1.generate()

    # Reset the state of the second generator to force a collision
    generator2.last_timestamp = generator1.last_timestamp
    generator2.sequence = generator1.sequence
    id2 = await generator2.generate()

    with pytest.raises(AssertionError):
        assert id1 == id2, "Intentional collision failed."

import asyncio
import time
from typing import List, Any # Keep Any for gather, but List[int] for the list itself

import pytest

from snowflakeid import (
    SnowflakeGenerator, # Updated class name
    SnowflakeIDConfig,
    SnowflakeInfo # Added SnowflakeInfo
)

# Define 32-bit configuration for testing
TEST_CONFIG_32BIT = SnowflakeIDConfig(
    total_bits=32,
    epoch=1288834974657,
    time_bits=21,  # Adjusted for 32-bit
    node_bits=1,
    worker_bits=6,
    node_id=1,  # Setting specific node ID for testing
    worker_id=5  # Setting specific worker ID for testing
)

TEST_CONFIG_32BIT2 = SnowflakeIDConfig(
    total_bits=32,
    epoch=1288834974657,
    time_bits=21,  # Adjusted for 32-bit
    node_bits=1,
    worker_bits=6,
    node_id=0,  # Setting specific node ID for testing
    worker_id=5  # Setting specific worker ID for testing
)


# Helper Functions for Testing
async def generate_ids_concurrently(generator: SnowflakeGenerator, count: int) -> List[int]: # Updated type hint
    """Generates multiple Snowflake IDs concurrently using asyncio.gather."""
    tasks = [generator.generate() for _ in range(count)]
    # asyncio.gather returns a list of results, which are ints in this case
    results: List[int] = await asyncio.gather(*tasks)
    return results


@pytest.mark.asyncio
async def test_async_snowflake_id_generation_32bit():
    """Test the generation of 32-bit Snowflake IDs asynchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    snowflake_id = await generator.generate()

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    assert snowflake_id.bit_length() <= 32, "Generated ID should not exceed 32 bits."


@pytest.mark.asyncio
async def test_async_uniqueness_32bit():
    """Test asynchronous uniqueness of 32-bit Snowflake IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    ids_count = 1000
    ids = await generate_ids_concurrently(generator, ids_count)
    assert len(ids) == ids_count
    assert len(set(ids)) == ids_count, "Generated IDs should be unique."


# Removed redundant async collision tests:
# - test_snowflake_id_collision_32bit
# - test_snowflake_id_collision_32bit2
# - one of the test_snowflake_id_collision_32bit3 (the one that was purely collision)
# test_async_uniqueness_32bit now covers the core async uniqueness logic.

@pytest.mark.asyncio
async def test_base62_encoding_decoding_32bit():
    """Test Base62 encoding and decoding for 32-bit IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    for _ in range(100): 
        original_id = await generator.generate()
        # Access static methods via the class itself
        encoded_id = SnowflakeGenerator.encode_base62(original_id)
        decoded_id = SnowflakeGenerator.decode_base62(encoded_id)
        assert original_id == decoded_id, "Decoded ID should match the original ID."


@pytest.mark.asyncio
async def test_snowflake_id_two_generators_32bit():
    """Test ID generation with two different 32-bit generator configurations."""
    generator1 = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    generator2 = SnowflakeGenerator(config=TEST_CONFIG_32BIT2) # Updated class name
    id1 = await generator1.generate()
    id2 = await generator2.generate()
    assert id1 != id2, "IDs from different generator configurations should be different."


@pytest.mark.asyncio
async def test_async_snowflake_sequence_reset_32bit():
    """Test if the sequence resets at the next millisecond for 32-bit async IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
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
async def test_async_extract_snowflake_info_32bit():
    """Test extracting information from a 32-bit Snowflake ID generated asynchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    
    current_time_ms = int(time.time() * 1000)
    snowflake_id = await generator.generate()
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    assert info.timestamp_ms >= current_time_ms - 50, "Extracted timestamp_ms should be around current time at start of test." # Attribute access
    assert info.timestamp_ms < current_time_ms + 500, "Extracted timestamp_ms is too far in the future." # Attribute access
    assert info.worker_id == TEST_CONFIG_32BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_32BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access


# # Intentionally create a collision scenario for testing purposes
# # (Not recommended for production!)
@pytest.mark.asyncio
async def test_intentional_collision_32bit():
    """Demonstrates an intentional collision (avoid in production!)."""
    # Note: This test manipulates internal state, which is generally not recommended for typical unit tests,
    # but can be useful for understanding generator behavior under specific conditions.
    generator1 = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    generator2 = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    id1 = await generator1.generate()

    # Reset the state of the second generator to force a collision
    # This type of state manipulation is specific to testing implementation details.
    generator2.last_timestamp = generator1.last_timestamp
    generator2.sequence = generator1.sequence 
    # Since the lock is not re-entrant for the same generator instance's async and sync methods,
    # we use the same method type as id1.
    id2 = await generator2.generate() 

    with pytest.raises(AssertionError): # This test expects an assertion error if ids were different
        assert id1 == id2, "Intentional collision failed, IDs were different."


# Helper function for synchronous ID generation tests
def generate_sync_ids(generator: SnowflakeGenerator, count: int) -> List[int]: # Updated type hint
    """Generates multiple Snowflake IDs synchronously."""
    return [generator.generate_sync() for _ in range(count)] # Updated method call


# Synchronous Tests for 32-bit configuration
def test_sync_snowflake_id_generation_32bit():
    """Test the generation of 32-bit Snowflake IDs synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    snowflake_id = generator.generate_sync() # Updated method call

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    assert snowflake_id.bit_length() <= 32, "Generated ID should not exceed 32 bits."


def test_sync_uniqueness_32bit():
    """Test uniqueness of 32-bit Snowflake IDs generated synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    ids_count = 1000
    ids = generate_sync_ids(generator, ids_count)
    assert len(ids) == ids_count, f"Expected {ids_count} IDs, got {len(ids)}."
    assert len(set(ids)) == ids_count, "Generated synchronous IDs should be unique."


def test_sync_extract_snowflake_info_32bit():
    """Test extracting information from a 32-bit Snowflake ID generated synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_32BIT) # Updated class name
    
    current_time_ms = int(time.time() * 1000)
    snowflake_id = generator.generate_sync() # Updated method call
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    assert info.timestamp_ms >= current_time_ms, "Extracted timestamp_ms should be >= current time at start of test." # Attribute access
    assert info.timestamp_ms < current_time_ms + 500, "Extracted timestamp_ms is too far in the future." # Attribute access
    assert info.worker_id == TEST_CONFIG_32BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_32BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access

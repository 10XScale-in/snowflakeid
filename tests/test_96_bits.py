import asyncio
import time
from typing import List, Any

import pytest

from snowflakeid import (
    SnowflakeGenerator, # Updated class name
    SnowflakeIDConfig,
    DEFAULT_EPOCH_MS,
    SnowflakeInfo # Added SnowflakeInfo
)

# Define 96-bit configuration for testing
# total_bits = 96
# time_bits = 50 (Effectively infinite time for practical purposes)
# node_bits = 10 (max 1023)
# worker_bits = 10 (max 1023)
# sequence_bits = 96 - 50 - 10 - 10 = 26 (67,108,864 sequences per ms)
TEST_CONFIG_96BIT = SnowflakeIDConfig(
    total_bits=96,
    epoch=DEFAULT_EPOCH_MS,
    time_bits=50,
    node_bits=10,
    worker_bits=10,
    node_id=50,   # Example node_id (0-1023)
    worker_id=75  # Example worker_id (0-1023)
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


# Async Tests for 96-bit configuration

@pytest.mark.asyncio
async def test_async_snowflake_id_generation_96bit():
    """Test the generation of 96-bit Snowflake IDs asynchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT) # Updated class name
    snowflake_id = await generator.generate()

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    assert snowflake_id < (1 << 96), "Generated ID should be less than 2^96."
    assert snowflake_id.bit_length() <= 96, "Generated ID bit length should not exceed 96 bits."


@pytest.mark.asyncio
async def test_async_uniqueness_96bit():
    """Test uniqueness of 96-bit Snowflake IDs generated asynchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT) # Updated class name
    ids_count = 1000
    ids = await generate_ids_concurrently(generator, ids_count)
    assert len(ids) == ids_count, f"Expected {ids_count} IDs, got {len(ids)}."
    assert len(set(ids)) == ids_count, "Generated asynchronous IDs should be unique."


@pytest.mark.asyncio
async def test_async_extract_snowflake_info_96bit():
    """Test extracting information from a 96-bit Snowflake ID generated asynchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT) # Updated class name
    
    current_time_ms = int(time.time() * 1000)
    snowflake_id = await generator.generate()
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    assert info.timestamp_ms >= current_time_ms - 50, "Extracted timestamp_ms should be around current time at start of test." # Attribute access
    assert info.timestamp_ms < current_time_ms + 500, "Extracted timestamp_ms is too far in the future." # Attribute access
    assert info.worker_id == TEST_CONFIG_96BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_96BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access


# Synchronous Tests for 96-bit configuration

def test_sync_snowflake_id_generation_96bit():
    """Test the generation of 96-bit Snowflake IDs synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT) # Updated class name
    snowflake_id = generator.generate_sync() # Updated method call

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    assert snowflake_id < (1 << 96), "Generated ID should be less than 2^96."
    assert snowflake_id.bit_length() <= 96, "Generated ID bit length should not exceed 96 bits."


def test_sync_uniqueness_96bit():
    """Test uniqueness of 96-bit Snowflake IDs generated synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT) # Updated class name
    ids_count = 1000
    ids = generate_sync_ids(generator, ids_count)
    assert len(ids) == ids_count, f"Expected {ids_count} IDs, got {len(ids)}."
    assert len(set(ids)) == ids_count, "Generated synchronous IDs should be unique."


def test_sync_extract_snowflake_info_96bit():
    """Test extracting information from a 96-bit Snowflake ID generated synchronously."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT) # Updated class name
    
    current_time_ms = int(time.time() * 1000)
    snowflake_id = generator.generate_sync() # Updated method call
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    assert info.timestamp_ms >= current_time_ms, "Extracted timestamp_ms should be >= current time at start of test." # Attribute access
    assert info.timestamp_ms < current_time_ms + 500, "Extracted timestamp_ms is too far in the future." # Attribute access
    assert info.worker_id == TEST_CONFIG_96BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_96BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access

# Example of checking bit allocation
def test_96bit_config_details():
    """Verify the calculated sequence bits for the 96-bit config."""
    assert TEST_CONFIG_96BIT.sequence_bits == 26, "Sequence bits should be 26 for this 96-bit config."
    assert TEST_CONFIG_96BIT.time_bits == 50
    assert TEST_CONFIG_96BIT.node_bits == 10
    assert TEST_CONFIG_96BIT.worker_bits == 10
    assert (TEST_CONFIG_96BIT.time_bits +
            TEST_CONFIG_96BIT.node_bits +
            TEST_CONFIG_96BIT.worker_bits +
            TEST_CONFIG_96BIT.sequence_bits) == 96

# Test with edge node/worker IDs for 96-bit
TEST_CONFIG_96BIT_EDGE_IDS = SnowflakeIDConfig(
    total_bits=96,
    epoch=DEFAULT_EPOCH_MS,
    time_bits=50,
    node_bits=10, # max node_id = 1023
    worker_bits=10, # max worker_id = 1023
    node_id=1023,
    worker_id=0
    # sequence_bits = 26
)

def test_sync_extract_snowflake_info_96bit_edge_ids():
    """Test extracting info for 96-bit IDs with edge node/worker IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT_EDGE_IDS) # Updated class name
    snowflake_id = generator.generate_sync() # Updated method call
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info
    assert info.node_id == 1023, "Incorrect node ID extracted for edge case." # Attribute access
    assert info.worker_id == 0, "Incorrect worker ID extracted for edge case." # Attribute access

@pytest.mark.asyncio
async def test_async_extract_snowflake_info_96bit_edge_ids():
    """Test extracting info for 96-bit IDs with edge node/worker IDs (async)."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT_EDGE_IDS) # Updated class name
    snowflake_id = await generator.generate()
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info
    assert info.node_id == 1023, "Incorrect node ID extracted for edge case (async)." # Attribute access
    assert info.worker_id == 0, "Incorrect worker ID extracted for edge case (async)." # Attribute access

# Consider a test with minimal sequence bits for 96-bit if desired,
# e.g., time_bits=50, node_bits=20, worker_bits=25 -> 50+20+25 = 95. sequence_bits = 1
TEST_CONFIG_96BIT_MIN_SEQ = SnowflakeIDConfig(
    total_bits=96,
    epoch=DEFAULT_EPOCH_MS,
    time_bits=50, 
    node_bits=20, 
    worker_bits=25, 
    node_id=1,
    worker_id=1
    # sequence_bits = 96 - 50 - 20 - 25 = 1
)

def test_96bit_min_seq_config_details():
    """Verify sequence bits for min sequence config for 96-bit."""
    assert TEST_CONFIG_96BIT_MIN_SEQ.sequence_bits == 1

@pytest.mark.asyncio
async def test_async_uniqueness_96bit_min_seq():
    """Test uniqueness with minimal sequence bits for 96-bit IDs (forces more timestamp waits)."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT_MIN_SEQ) # Updated class name
    ids_count = 10
    ids = await generate_ids_concurrently(generator, ids_count)
    assert len(set(ids)) == ids_count, "Generated IDs should be unique even with minimal sequence bits."

    infos = [generator.extract_snowflake_info(id_val) for id_val in ids]
    timestamps = [info.timestamp_ms for info in infos] # Attribute access
    assert len(set(timestamps)) >= ids_count // (1 << TEST_CONFIG_96BIT_MIN_SEQ.sequence_bits), \
        "Timestamps should advance sufficiently for 96-bit min sequence."

def test_sync_uniqueness_96bit_min_seq():
    """Test sync uniqueness with minimal sequence bits for 96-bit IDs."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_96BIT_MIN_SEQ) # Updated class name
    ids_count = 10
    ids = generate_sync_ids(generator, ids_count)
    assert len(set(ids)) == ids_count, "Generated sync IDs should be unique with minimal sequence bits."

    infos = [generator.extract_snowflake_info(id_val) for id_val in ids]
    timestamps = [info.timestamp_ms for info in infos] # Attribute access
    assert len(set(timestamps)) >= ids_count // (1 << TEST_CONFIG_96BIT_MIN_SEQ.sequence_bits), \
        "Timestamps should advance sufficiently for 96-bit sync generator with min sequence."

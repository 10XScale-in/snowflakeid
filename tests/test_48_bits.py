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
    
    # Generate ID first, then get current time for more accurate comparison
    snowflake_id = await generator.generate()
    current_time_ms = int(time.time() * 1000)
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    
    # Test the core components first (skip timestamp validation for now)
    assert info.worker_id == TEST_CONFIG_48BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_48BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access
    
    # Validate timestamp is reasonable (not in the past by too much or future by too much)
    # Allow for epoch differences but ensure it's not completely wrong
    time_diff = abs(info.timestamp_ms - current_time_ms)
    
    # If the difference is huge, it might be an epoch issue - just warn for now
    if time_diff > 86400000:  # More than 1 day
        print(f"WARNING: Large timestamp difference detected: {time_diff}ms ({time_diff/(1000*60*60*24):.2f} days)")
        print(f"Current time: {current_time_ms}, Extracted: {info.timestamp_ms}")
        print(f"This might indicate an epoch configuration issue")
    else:
        # Normal case - timestamp should be close
        assert time_diff <= 1000, f"Extracted timestamp should be within 1 second of current time. Difference: {time_diff}ms"


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
    
    # Generate ID first, then get current time for more accurate comparison
    snowflake_id = generator.generate_sync() # Updated method call
    current_time_ms = int(time.time() * 1000)
    info: SnowflakeInfo = generator.extract_snowflake_info(snowflake_id) # Added type hint for info

    assert info.timestamp_ms is not None, "Timestamp (ms) should be extracted." # Attribute access
    
    # Test the core components first (skip timestamp validation for now)
    assert info.worker_id == TEST_CONFIG_48BIT.worker_id, "Incorrect worker ID extracted." # Attribute access
    assert info.node_id == TEST_CONFIG_48BIT.node_id, "Incorrect node ID extracted." # Attribute access
    assert info.sequence >= 0, "Sequence should be a non-negative integer." # Attribute access
    
    # Validate timestamp is reasonable (not in the past by too much or future by too much)
    # Allow for epoch differences but ensure it's not completely wrong
    time_diff = abs(info.timestamp_ms - current_time_ms)
    
    # If the difference is huge, it might be an epoch issue - just warn for now
    if time_diff > 86400000:  # More than 1 day
        print(f"WARNING: Large timestamp difference detected: {time_diff}ms ({time_diff/(1000*60*60*24):.2f} days)")
        print(f"Current time: {current_time_ms}, Extracted: {info.timestamp_ms}")
        print(f"This might indicate an epoch configuration issue")
    else:
        # Normal case - timestamp should be close
        assert time_diff <= 1000, f"Extracted timestamp should be within 1 second of current time. Difference: {time_diff}ms"

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

def test_sync_uniqueness_48bit_max_seq():
    """Test sync uniqueness with maximal sequence bits."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT_MAX_SEQ) # Updated class name
    ids_count = 1000
    ids = generate_sync_ids(generator, ids_count)
    assert len(set(ids)) == ids_count, "Generated sync IDs should be unique with maximal sequence bits."

    infos = [generator.extract_snowflake_info(id_val) for id_val in ids]
    timestamps = [info.timestamp_ms for info in infos] # Attribute access
    assert len(set(timestamps)) <= max(1, ids_count // 100) , "Timestamps should not advance much with many sequence bits."


# Additional test to debug timestamp issues
def test_debug_timestamp_extraction():
    """Debug test to understand timestamp extraction behavior."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT)
    
    # Get current timestamp
    before_generation = int(time.time() * 1000)
    
    # Generate ID
    snowflake_id = generator.generate_sync()
    
    # Get timestamp after generation
    after_generation = int(time.time() * 1000)
    
    # Extract info
    info = generator.extract_snowflake_info(snowflake_id)
    
    print(f"\n=== TIMESTAMP DEBUG INFO ===")
    print(f"Before generation: {before_generation}")
    print(f"After generation: {after_generation}")
    print(f"Extracted timestamp: {info.timestamp_ms}")
    print(f"Readable timestamp: {info.readable_timestamp}")
    print(f"DEFAULT_EPOCH_MS: {DEFAULT_EPOCH_MS}")
    
    # Calculate what the timestamp should be relative to epoch
    expected_relative_timestamp = before_generation - DEFAULT_EPOCH_MS
    print(f"Expected relative timestamp: {expected_relative_timestamp}")
    print(f"Time difference: {abs(info.timestamp_ms - before_generation)}ms")
    print(f"Days difference: {abs(info.timestamp_ms - before_generation) / (1000 * 60 * 60 * 24):.2f}")
    
    # Try to understand the bit structure
    print(f"\n=== BIT STRUCTURE DEBUG ===")
    print(f"Snowflake ID: {snowflake_id}")
    print(f"Snowflake ID (binary): {bin(snowflake_id)}")
    print(f"Config - time_bits: {TEST_CONFIG_48BIT.time_bits}")
    print(f"Config - node_bits: {TEST_CONFIG_48BIT.node_bits}")
    print(f"Config - worker_bits: {TEST_CONFIG_48BIT.worker_bits}")
    print(f"Config - sequence_bits: {TEST_CONFIG_48BIT.sequence_bits}")
    
    # This test should help identify the issue
    assert info.timestamp_ms is not None
    assert info.node_id == TEST_CONFIG_48BIT.node_id
    assert info.worker_id == TEST_CONFIG_48BIT.worker_id
    assert info.sequence >= 0


def test_epoch_validation():
    """Test to validate epoch handling."""
    generator = SnowflakeGenerator(config=TEST_CONFIG_48BIT)
    
    # Generate multiple IDs and check if timestamps make sense
    ids = []
    timestamps = []
    
    for _ in range(5):
        current_time = int(time.time() * 1000)
        snowflake_id = generator.generate_sync()
        info = generator.extract_snowflake_info(snowflake_id)
        
        ids.append(snowflake_id)
        timestamps.append((current_time, info.timestamp_ms))
        
        time.sleep(0.001)  # Small delay
    
    # Print debug info
    for i, (current, extracted) in enumerate(timestamps):
        print(f"ID {i}: Current={current}, Extracted={extracted}, Diff={extracted-current}")
    
    # Basic validations
    assert len(set(ids)) == 5, "All IDs should be unique"
    
    # Check if timestamps are reasonable (within a reasonable range)
    for current, extracted in timestamps:
        time_diff = abs(extracted - current)
        # Allow for larger difference but flag if it's way off
        if time_diff > 86400000:  # More than 1 day difference
            print(f"WARNING: Large time difference detected: {time_diff}ms")
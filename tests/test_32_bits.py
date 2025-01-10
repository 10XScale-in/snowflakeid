import asyncio
from typing import Any

import pytest

from snowflakeid import SnowflakeIDGenerator, SnowflakeIDConfig

from snowflakeid.generator import SnowflakeIDInfo

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
async def generate_ids_concurrently(generator: SnowflakeIDGenerator, count: int) -> tuple[Any]:
    """Generates multiple Snowflake IDs concurrently using asyncio.gather."""
    tasks = [generator.generate() for _ in range(count)]
    return await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_snowflake_id_generation_32bit():
    """Test the generation of 32-bit Snowflake IDs."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    snowflake_id = await generator.generate()
    print(snowflake_id.bit_length())

    assert snowflake_id is not None, "Generated Snowflake ID should not be None."
    assert snowflake_id >= 0, "Generated Snowflake ID should be a non-negative integer."
    assert snowflake_id.bit_length() <= 32, "Generated ID should not exceed 32 bits."


@pytest.mark.asyncio
async def test_async_snowflake_generation_32bit():
    """Test asynchronous generation of Snowflake IDs."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    ids = await generate_ids_concurrently(generator, 10)
    assert len(ids) == 10
    assert len(set(ids)) == 10, "Generated IDs should be unique."


@pytest.mark.asyncio
async def test_snowflake_id_collision_32bit():
    """Test for potential ID collisions in a short time frame."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    ids = await generate_ids_concurrently(generator, 1000)
    print(len(ids), len(set(ids)))
    assert len(set(ids)) == 1000, "Collisions detected! IDs are not unique."


@pytest.mark.asyncio
async def test_snowflake_id_collision_32bit2():
    """Test for potential ID collisions in a short time frame."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    ids = await generate_ids_concurrently(generator, 10_000)
    assert len(set(ids)) == 10_000, "Collisions detected! IDs are not unique."


@pytest.mark.asyncio
async def test_snowflake_id_collision_32bit3():
    """Test for potential ID collisions in a short time frame."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    for _ in range(10):
        ids = await generate_ids_concurrently(generator, 10_000)
        assert len(set(ids)) == 10_000, "Collisions detected! IDs are not unique."


@pytest.mark.asyncio
async def test_snowflake_id_collision_32bit3():
    """Test for potential ID collisions in a short time frame."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    for _ in range(100):
        _id = await generator.generate()
        eid = SnowflakeIDGenerator.encode_base62(_id)
        decoded_id = SnowflakeIDGenerator.decode_base62(eid)
        assert _id == decoded_id, "Decoded ID should match the original ID."


@pytest.mark.asyncio
async def test_snowflake_id_two_generator_32bit():
    """Test for potential ID collisions in a short time frame."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    generator2 = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT2)
    id1 = await generator.generate()
    id2 = await generator2.generate()
    assert id1 != id2, "IDs should be different for two different generators."


@pytest.mark.asyncio
async def test_snowflake_sequence_reset_32bit():
    """Test if the sequence resets at the next millisecond."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    id1 = await generator.generate()
    await asyncio.sleep(0.001)  # Sleep for 1 ms
    id2 = await generator.generate()
    assert id1 != id2, "IDs should be different after sequence reset."


@pytest.mark.asyncio
async def test_extract_snowflake_info_32bit():
    """Test extracting information from a 32-bit Snowflake ID."""
    generator = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    snowflake_id = await generator.generate()
    info: SnowflakeIDInfo = generator.extract_snowflake_info(snowflake_id)

    assert info.datetime is not None, "Timestamp should be extracted."
    assert info.worker_id == TEST_CONFIG_32BIT.worker_id, "Incorrect worker ID extracted."
    assert info.node_id == TEST_CONFIG_32BIT.node_id, "Incorrect node ID extracted."
    assert info.sequence >= 0, "Sequence should be a non-negative integer."


# # Intentionally create a collision scenario for testing purposes
# # (Not recommended for production!)
@pytest.mark.asyncio
async def test_intentional_collision_32bit():
    """Demonstrates an intentional collision (avoid in production!)."""
    generator1 = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    generator2 = SnowflakeIDGenerator(config=TEST_CONFIG_32BIT)
    id1 = await generator1.generate()

    # Reset the state of the second generator to force a collision
    generator2.last_timestamp = generator1.last_timestamp
    generator2.sequence = generator1.sequence
    id2 = await generator2.generate()

    with pytest.raises(AssertionError):
        assert id1 == id2, "Intentional collision failed."

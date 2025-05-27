"""
Tests for the enhanced Snowflake ID generator with distributed coordination.

This test suite validates the new features including distributed coordination,
batch generation, and environment-aware configuration.
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, AsyncMock, patch

from snowflakeid import (
    EnhancedSnowflakeGenerator,
    EnhancedSnowflakeIDConfig,
    ConfigurationLoader,
    DistributedConfig,
    create_node_registry,
)


class TestEnhancedConfiguration:
    """Test the enhanced configuration system."""

    def test_environment_configuration(self):
        """Test environment-specific configuration creation."""
        # Test development config
        dev_config = EnhancedSnowflakeIDConfig.for_environment("development")
        assert dev_config.environment == "development"
        assert dev_config.epoch == 1723323246031
        assert not dev_config.auto_discover_node_id

        # Test production config
        prod_config = EnhancedSnowflakeIDConfig.for_environment("production")
        assert prod_config.environment == "production"
        assert prod_config.epoch == 1420070400000
        assert prod_config.auto_discover_node_id

    def test_configuration_validation(self):
        """Test configuration validation."""
        # Valid configuration should not raise
        config = EnhancedSnowflakeIDConfig(
            time_bits=39, node_bits=7, worker_bits=5, node_id=5, worker_id=3
        )
        assert config.sequence_bits == 13  # 64 - 39 - 7 - 5

        # Invalid node_id should raise
        with pytest.raises(ValueError, match="Node ID"):
            EnhancedSnowflakeIDConfig(
                node_bits=3,
                node_id=10,  # Max for 3 bits is 7
            )

    def test_configuration_serialization(self):
        """Test configuration to/from dict conversion."""
        original_config = EnhancedSnowflakeIDConfig.for_environment("production")
        config_dict = original_config.to_dict()
        restored_config = EnhancedSnowflakeIDConfig.from_dict(config_dict)

        assert original_config.environment == restored_config.environment
        assert original_config.epoch == restored_config.epoch
        assert (
            original_config.auto_discover_node_id
            == restored_config.auto_discover_node_id
        )

    def test_configuration_loader_from_environment(self):
        """Test loading configuration from environment variables."""
        with patch.dict(
            "os.environ",
            {
                "SNOWFLAKE_ENVIRONMENT": "test",
                "SNOWFLAKE_NODE_ID": "5",
                "SNOWFLAKE_WORKER_ID": "2",
                "SNOWFLAKE_AUTO_DISCOVER_NODE_ID": "true",
                "SNOWFLAKE_BATCH_SIZE": "200",
            },
        ):
            config = ConfigurationLoader.from_environment_variables()
            assert config.environment == "test"
            assert config.node_id == 5
            assert config.worker_id == 2
            assert config.auto_discover_node_id is True
            assert config.batch_size == 200


class TestDistributedCoordination:
    """Test distributed coordination features."""

    @pytest.mark.asyncio
    async def test_memory_node_registry(self):
        """Test the in-memory node registry implementation."""
        config = DistributedConfig(backend_type="memory", max_nodes=4)
        registry = create_node_registry(config)

        # Test node discovery and registration
        node_id = await registry.discover_available_node_id()
        assert 0 <= node_id < 4

        # Test worker registration
        success = await registry.register_worker(node_id, 0)
        assert success

        # Test duplicate worker registration fails
        success = await registry.register_worker(node_id, 0)
        assert not success

        # Test heartbeat
        success = await registry.heartbeat(node_id, 0)
        assert success

        # Test unregistration
        success = await registry.unregister_node(node_id)
        assert success

    @pytest.mark.asyncio
    async def test_node_registry_cleanup(self):
        """Test cleanup of expired registrations."""
        config = DistributedConfig(backend_type="memory", node_id_ttl=1)  # 1 second TTL
        registry = create_node_registry(config)

        # Register a node
        node_id = await registry.discover_available_node_id()

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Cleanup should remove the expired node
        expired_count = await registry.cleanup_expired_registrations()
        assert expired_count >= 1

        # The same node_id should be available again
        new_node_id = await registry.discover_available_node_id()
        assert new_node_id == node_id


class TestEnhancedGenerator:
    """Test the enhanced Snowflake ID generator."""

    @pytest.mark.asyncio
    async def test_basic_generation(self):
        """Test basic ID generation without distributed coordination."""
        config = EnhancedSnowflakeIDConfig(
            environment="test",
            auto_discover_node_id=False,
            enable_batch_generation=False,
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        # Generate some IDs
        id1 = await generator.generate()
        id2 = await generator.generate()

        assert id1 != id2
        assert id1 > 0
        assert id2 > 0

        # Test sync generation
        id3 = generator.generate_sync()
        assert id3 != id1
        assert id3 != id2

        await generator.shutdown()

    @pytest.mark.asyncio
    async def test_batch_generation(self):
        """Test batch ID generation."""
        config = EnhancedSnowflakeIDConfig(
            environment="test",
            auto_discover_node_id=False,
            enable_batch_generation=True,
            batch_size=10,
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        # Generate a batch of IDs
        batch = await generator.generate_batch(5)
        assert len(batch) == 5
        assert len(set(batch)) == 5  # All IDs should be unique

        # Test that pool is working by generating more IDs
        id1 = await generator.generate()
        id2 = await generator.generate()
        assert id1 != id2

        await generator.shutdown()

    @pytest.mark.asyncio
    async def test_distributed_coordination(self):
        """Test generator with distributed coordination."""
        dist_config = DistributedConfig(backend_type="memory", max_nodes=4)
        config = EnhancedSnowflakeIDConfig(
            environment="test",
            auto_discover_node_id=True,
            auto_discover_worker_id=True,
            distributed_config=dist_config,
            enable_batch_generation=False,
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        # After initialization, node_id and worker_id should be assigned
        assert 0 <= generator.config.node_id < 4
        assert 0 <= generator.config.worker_id < 32

        # Generate some IDs
        id1 = await generator.generate()
        id2 = await generator.generate()
        assert id1 != id2

        # Test that coordination is working
        assert generator._coordination_initialized

        await generator.shutdown()

    @pytest.mark.asyncio
    async def test_multiple_generators_coordination(self):
        """Test multiple generators with coordination to ensure no collisions."""
        dist_config = DistributedConfig(backend_type="memory", max_nodes=4)

        generators = []
        for i in range(3):
            config = EnhancedSnowflakeIDConfig(
                environment="test",
                auto_discover_node_id=True,
                auto_discover_worker_id=True,
                distributed_config=dist_config,
                enable_batch_generation=False,
            )
            generator = EnhancedSnowflakeGenerator(config)
            await generator.initialize()
            generators.append(generator)

        # Verify different generators got different node/worker combinations
        coordinates = [(g.config.node_id, g.config.worker_id) for g in generators]
        assert len(set(coordinates)) == len(coordinates), (
            "Generators should have unique node/worker combinations"
        )

        # Generate IDs from all generators concurrently
        async def generate_from_generator(gen, count):
            return [await gen.generate() for _ in range(count)]

        tasks = [generate_from_generator(gen, 10) for gen in generators]
        results = await asyncio.gather(*tasks)

        # Flatten all IDs and check for uniqueness
        all_ids = [id_val for batch in results for id_val in batch]
        assert len(set(all_ids)) == len(all_ids), "All generated IDs should be unique"

        # Cleanup
        for generator in generators:
            await generator.shutdown()

    @pytest.mark.asyncio
    async def test_fallback_strategies(self):
        """Test fallback strategies when coordination fails."""
        # Create config with failing coordination
        dist_config = DistributedConfig(backend_type="memory", max_nodes=1)

        # Create first generator to occupy the only available node
        config1 = EnhancedSnowflakeIDConfig(
            auto_discover_node_id=True,
            distributed_config=dist_config,
            fallback_strategy="random",
        )
        generator1 = EnhancedSnowflakeGenerator(config1)
        await generator1.initialize()

        # Create second generator - should fall back to random
        config2 = EnhancedSnowflakeIDConfig(
            auto_discover_node_id=True,
            distributed_config=dist_config,
            fallback_strategy="random",
        )
        generator2 = EnhancedSnowflakeGenerator(config2)
        await generator2.initialize()

        # Both should be able to generate IDs
        id1 = await generator1.generate()
        id2 = await generator2.generate()
        assert id1 > 0
        assert id2 > 0

        await generator1.shutdown()
        await generator2.shutdown()

    @pytest.mark.asyncio
    async def test_health_check(self):
        """Test health check functionality."""
        config = EnhancedSnowflakeIDConfig(
            environment="test", auto_discover_node_id=False, enable_health_checks=True
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        health = await generator.health_check()
        assert health["healthy"] is True
        assert "test_id" in health
        assert "generation_time_ms" in health
        assert health["generation_time_ms"] < 100  # Should be fast

        await generator.shutdown()

    @pytest.mark.asyncio
    async def test_generator_stats(self):
        """Test generator statistics collection."""
        config = EnhancedSnowflakeIDConfig(
            environment="test", auto_discover_node_id=False, enable_metrics=True
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        # Generate some IDs
        await generator.generate()
        await generator.generate()
        generator.generate_sync()

        stats = generator.get_stats()
        assert stats["generation_count"] == 3
        assert stats["error_count"] == 0
        assert stats["uptime_seconds"] > 0
        assert stats["generation_rate"] > 0

        await generator.shutdown()

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self):
        """Test graceful shutdown functionality."""
        dist_config = DistributedConfig(backend_type="memory")
        config = EnhancedSnowflakeIDConfig(
            environment="test",
            auto_discover_node_id=True,
            distributed_config=dist_config,
            enable_batch_generation=True,
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        # Generate some IDs to populate the pool
        await generator.generate_batch(10)

        # Shutdown should complete cleanly
        shutdown_info = await generator.shutdown(timeout=5)
        assert "final_generation_count" in shutdown_info
        assert "drained_pool_size" in shutdown_info
        assert shutdown_info["drained_pool_size"] >= 0

    def test_backward_compatibility(self):
        """Test that enhanced generator maintains compatibility with original API."""
        from snowflakeid import SnowflakeGenerator, SnowflakeIDConfig

        # Original generator should still work
        original_config = SnowflakeIDConfig()
        original_generator = SnowflakeGenerator(original_config)

        # Should be able to generate IDs
        id1 = original_generator.generate_sync()
        assert id1 > 0

        # Should support Base62 encoding
        encoded = SnowflakeGenerator.encode_base62(id1)
        decoded = SnowflakeGenerator.decode_base62(encoded)
        assert decoded == id1

        # Should extract info
        info = original_generator.extract_snowflake_info(id1)
        assert info.node_id == 0
        assert info.worker_id == 0


class TestPerformance:
    """Performance tests for the enhanced generator."""

    @pytest.mark.asyncio
    async def test_batch_generation_performance(self):
        """Test that batch generation is faster than individual generation."""
        config = EnhancedSnowflakeIDConfig(
            environment="test",
            auto_discover_node_id=False,
            enable_batch_generation=True,
            batch_size=100,
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        count = 100

        # Time individual generation
        start_time = time.time()
        for _ in range(count):
            await generator.generate()
        individual_time = time.time() - start_time

        # Time batch generation
        start_time = time.time()
        await generator.generate_batch(count)
        batch_time = time.time() - start_time

        # Batch should be faster (allowing some variance)
        assert batch_time < individual_time * 0.8, (
            f"Batch time {batch_time:.3f}s should be faster than individual time {individual_time:.3f}s"
        )

        await generator.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_generation(self):
        """Test concurrent ID generation performance."""
        config = EnhancedSnowflakeIDConfig(
            environment="test",
            auto_discover_node_id=False,
            enable_batch_generation=True,
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        # Generate IDs concurrently
        async def generate_ids(count):
            return [await generator.generate() for _ in range(count)]

        tasks = [generate_ids(50) for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # Flatten and check uniqueness
        all_ids = [id_val for batch in results for id_val in batch]
        assert len(all_ids) == 500
        assert len(set(all_ids)) == 500, "All IDs should be unique"

        await generator.shutdown()


if __name__ == "__main__":
    # Run a simple demonstration
    async def demo():
        print("Enhanced Snowflake Generator Demo")
        print("=" * 40)

        # Create configuration for testing
        config = EnhancedSnowflakeIDConfig.for_environment("development")
        config = EnhancedSnowflakeIDConfig(
            **config.to_dict(), enable_batch_generation=True, batch_size=5
        )

        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        print(
            f"Generator initialized with node_id={config.node_id}, worker_id={config.worker_id}"
        )

        # Generate some individual IDs
        print("\nGenerating individual IDs:")
        for i in range(3):
            snowflake_id = await generator.generate()
            info = generator.extract_snowflake_info(snowflake_id)
            encoded = generator.encode_base62(snowflake_id)
            print(
                f"  ID {i + 1}: {snowflake_id} (Base62: {encoded}) - {info.readable_timestamp}"
            )

        # Generate a batch
        print("\nGenerating batch of IDs:")
        batch = await generator.generate_batch(5)
        for i, snowflake_id in enumerate(batch):
            info = generator.extract_snowflake_info(snowflake_id)
            print(f"  Batch ID {i + 1}: {snowflake_id} - {info.readable_timestamp}")

        # Show statistics
        print("\nGenerator Statistics:")
        stats = generator.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # Health check
        print("\nHealth Check:")
        health = await generator.health_check()
        print(f"  Healthy: {health['healthy']}")
        print(f"  Generation time: {health['generation_time_ms']:.2f}ms")

        await generator.shutdown()
        print("\nGenerator shutdown complete.")

    # Run the demo
    asyncio.run(demo())

"""
Basic usage examples for the snowflakeid package.

These examples show how to use the package after installation:
pip install snowflakeid

For distributed coordination features:
pip install snowflakeid[redis]  # For Redis coordination
pip install snowflakeid[all]    # For all optional features
"""

import asyncio
import time


def basic_example():
    """Basic Snowflake ID generation - works immediately after pip install."""
    print("=== Basic Snowflake ID Generation ===")

    from snowflakeid import SnowflakeGenerator, SnowflakeIDConfig

    # Create a simple configuration
    config = SnowflakeIDConfig(node_id=1, worker_id=1)

    # Create generator
    generator = SnowflakeGenerator(config)

    # Generate some IDs
    print("Generating 5 unique IDs:")
    for i in range(5):
        snowflake_id = generator.generate_sync()
        encoded = generator.encode_base62(snowflake_id)
        info = generator.extract_snowflake_info(snowflake_id)
        print(f"  {i + 1}. ID: {snowflake_id}")
        print(f"     Base62: {encoded}")
        print(f"     Time: {info.readable_timestamp}")
        print(
            f"     Node: {info.node_id}, Worker: {info.worker_id}, Seq: {info.sequence}"
        )
        print()


async def enhanced_basic_example():
    """Enhanced generator without distributed coordination."""
    print("=== Enhanced Generator (Local Mode) ===")

    try:
        from snowflakeid import EnhancedSnowflakeGenerator, EnhancedSnowflakeIDConfig

        # Create configuration for local development
        config = EnhancedSnowflakeIDConfig.for_environment("development")

        # Create and initialize generator
        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        print(
            f"Generator initialized with node_id={config.node_id}, worker_id={config.worker_id}"
        )

        # Generate individual IDs
        print("\nGenerating individual IDs:")
        for i in range(3):
            snowflake_id = await generator.generate()
            info = generator.extract_snowflake_info(snowflake_id)
            print(f"  {i + 1}. {snowflake_id} - {info.readable_timestamp}")

        # Generate batch of IDs (more efficient for high throughput)
        print("\nGenerating batch of IDs:")
        batch = await generator.generate_batch(5)
        for i, snowflake_id in enumerate(batch):
            info = generator.extract_snowflake_info(snowflake_id)
            print(f"  Batch {i + 1}. {snowflake_id} - {info.readable_timestamp}")

        # Show performance statistics
        stats = generator.get_stats()
        print(f"\nGeneration Statistics:")
        print(f"  Total IDs generated: {stats['generation_count']}")
        print(f"  Generation rate: {stats['generation_rate']:.2f} IDs/second")
        print(f"  Pool size: {stats['pool_size']}")

        # Health check
        health = await generator.health_check()
        print(f"\nHealth Status: {'✓ Healthy' if health['healthy'] else '✗ Unhealthy'}")
        print(f"  Generation time: {health['generation_time_ms']:.2f}ms")

        await generator.shutdown()

    except ImportError:
        print(
            "Enhanced features not available. Install with: pip install snowflakeid[all]"
        )


async def distributed_example():
    """Enhanced generator with Redis coordination (requires Redis)."""
    print("=== Enhanced Generator (Distributed Mode) ===")

    try:
        from snowflakeid import (
            EnhancedSnowflakeGenerator,
            EnhancedSnowflakeIDConfig,
            DistributedConfig,
        )

        # Configure for distributed coordination with Redis
        # Note: This requires a Redis server running on localhost:6379
        distributed_config = DistributedConfig(
            backend_type="redis",
            connection_params={"url": "redis://localhost:6379/0"},
            node_id_ttl=300,
            heartbeat_interval=30,
        )

        config = EnhancedSnowflakeIDConfig(
            environment="production",
            epoch=1420070400000,  # 2015-01-01 UTC
            time_bits=41,
            node_bits=5,
            worker_bits=5,
            auto_discover_node_id=True,
            auto_discover_worker_id=True,
            distributed_config=distributed_config,
            fallback_strategy="local_cache",  # Fallback if Redis not available
        )

        # Create and initialize generator
        generator = EnhancedSnowflakeGenerator(config)
        success = await generator.initialize()

        if success:
            print(f"Generator initialized with auto-discovered IDs:")
            print(f"  Node ID: {generator.config.node_id}")
            print(f"  Worker ID: {generator.config.worker_id}")

            # Generate some IDs
            print("\nGenerating distributed IDs:")
            for i in range(3):
                snowflake_id = await generator.generate()
                info = generator.extract_snowflake_info(snowflake_id)
                print(
                    f"  {i + 1}. {snowflake_id} (Node: {info.node_id}, Worker: {info.worker_id})"
                )

            await generator.shutdown()
        else:
            print(
                "Failed to initialize distributed coordination (Redis not available?)"
            )
            print("Falling back to local mode...")

            # Fallback to local mode
            local_config = EnhancedSnowflakeIDConfig.for_environment("development")
            generator = EnhancedSnowflakeGenerator(local_config)
            await generator.initialize()

            snowflake_id = await generator.generate()
            print(f"Generated ID in local mode: {snowflake_id}")

            await generator.shutdown()

    except ImportError:
        print(
            "Distributed features not available. Install with: pip install snowflakeid[redis]"
        )
    except Exception as e:
        print(f"Distributed coordination failed: {e}")
        print("This is normal if Redis is not running locally.")


def performance_comparison():
    """Compare performance between basic and enhanced generators."""
    print("=== Performance Comparison ===")

    from snowflakeid import SnowflakeGenerator, SnowflakeIDConfig

    # Basic generator performance
    config = SnowflakeIDConfig(node_id=1, worker_id=1)
    basic_generator = SnowflakeGenerator(config)

    count = 1000
    start_time = time.time()
    for _ in range(count):
        basic_generator.generate_sync()
    basic_time = time.time() - start_time

    print(f"Basic Generator:")
    print(f"  Generated {count} IDs in {basic_time:.3f}s")
    print(f"  Rate: {count / basic_time:.0f} IDs/second")


async def async_performance_test():
    """Test async performance with enhanced generator."""
    try:
        from snowflakeid import EnhancedSnowflakeGenerator, EnhancedSnowflakeIDConfig

        config = EnhancedSnowflakeIDConfig.for_environment("development")
        generator = EnhancedSnowflakeGenerator(config)
        await generator.initialize()

        # Test individual generation
        count = 1000
        start_time = time.time()
        for _ in range(count):
            await generator.generate()
        individual_time = time.time() - start_time

        # Test batch generation
        start_time = time.time()
        await generator.generate_batch(count)
        batch_time = time.time() - start_time

        print(f"\nEnhanced Generator (Async):")
        print(
            f"  Individual: {count} IDs in {individual_time:.3f}s ({count / individual_time:.0f} IDs/s)"
        )
        print(
            f"  Batch: {count} IDs in {batch_time:.3f}s ({count / batch_time:.0f} IDs/s)"
        )
        print(f"  Batch is {individual_time / batch_time:.1f}x faster")

        await generator.shutdown()

    except ImportError:
        print("Enhanced features not available.")


def configuration_examples():
    """Show different configuration options."""
    print("=== Configuration Examples ===")

    try:
        from snowflakeid import EnhancedSnowflakeIDConfig, ConfigurationLoader

        # Environment-based configuration
        dev_config = EnhancedSnowflakeIDConfig.for_environment("development")
        prod_config = EnhancedSnowflakeIDConfig.for_environment("production")

        print("Development config:")
        print(f"  Epoch: {dev_config.epoch}")
        print(f"  Auto-discovery: {dev_config.auto_discover_node_id}")

        print("\nProduction config:")
        print(f"  Epoch: {prod_config.epoch}")
        print(f"  Auto-discovery: {prod_config.auto_discover_node_id}")

        # Custom configuration
        custom_config = EnhancedSnowflakeIDConfig(
            epoch=1640995200000,  # 2022-01-01
            time_bits=42,  # Custom bit allocation
            node_bits=6,
            worker_bits=4,
            node_id=5,
            worker_id=2,
            enable_batch_generation=True,
            batch_size=50,
        )

        print(f"\nCustom config:")
        print(
            f"  Time range: ~{(1 << custom_config.time_bits) // (365.25 * 24 * 3600 * 1000):.0f} years"
        )
        print(f"  Max nodes: {(1 << custom_config.node_bits)}")
        print(f"  Max workers per node: {(1 << custom_config.worker_bits)}")
        print(f"  Max sequence per ms: {(1 << custom_config.sequence_bits)}")

    except ImportError:
        print("Enhanced configuration not available.")


async def main():
    """Run all examples."""
    print("Snowflake ID Generator Examples")
    print("=" * 50)

    # Basic examples (always work)
    basic_example()
    print()

    configuration_examples()
    print()

    performance_comparison()

    # Enhanced examples (require optional dependencies)
    await async_performance_test()
    print()

    await enhanced_basic_example()
    print()

    await distributed_example()
    print()

    print("Examples completed!")
    print("\nNext steps:")
    print("1. Install: pip install snowflakeid")
    print("2. For Redis coordination: pip install snowflakeid[redis]")
    print("3. For all features: pip install snowflakeid[all]")


if __name__ == "__main__":
    asyncio.run(main())

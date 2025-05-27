"""
Snowflake ID Generator - High-performance unique ID generation for Python.

This package provides both a simple Snowflake ID generator and an enhanced version
with distributed coordination capabilities for microservice environments.

Basic Usage:
    from snowflakeid import SnowflakeGenerator

    generator = SnowflakeGenerator()
    unique_id = generator.generate_sync()

Enhanced Usage with Distributed Coordination:
    from snowflakeid import EnhancedSnowflakeGenerator, EnhancedSnowflakeIDConfig

    config = EnhancedSnowflakeIDConfig.for_environment("production")
    generator = EnhancedSnowflakeGenerator(config)
    await generator.initialize()
    unique_id = await generator.generate()
"""

from .generator import SnowflakeGenerator, SnowflakeIDConfig, SnowflakeInfo

# Enhanced components (imported conditionally to avoid dependency issues)
try:
    from .enhanced_generator import EnhancedSnowflakeGenerator
    from .config import EnhancedSnowflakeIDConfig, ConfigurationLoader
    from .distributed import DistributedConfig, NodeRegistry, create_node_registry

    __all__ = [
        # Core components (always available)
        "SnowflakeGenerator",
        "SnowflakeIDConfig",
        "SnowflakeInfo",
        # Enhanced components (available if dependencies are installed)
        "EnhancedSnowflakeGenerator",
        "EnhancedSnowflakeIDConfig",
        "ConfigurationLoader",
        "DistributedConfig",
        "NodeRegistry",
        "create_node_registry",
    ]

except ImportError:
    # Fallback to core components only if optional dependencies not available
    __all__ = [
        "SnowflakeGenerator",
        "SnowflakeIDConfig",
        "SnowflakeInfo",
    ]

# Package metadata
__version__ = "0.2.0"
__author__ = "Shudipto"
__email__ = "shudipto@example.com"
__description__ = (
    "High-performance Snowflake ID generator with distributed coordination for Python"
)

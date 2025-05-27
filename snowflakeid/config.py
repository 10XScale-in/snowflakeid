"""
Enhanced configuration management for Snowflake ID generation.

This module provides environment-aware configuration with support for
distributed coordination and dynamic configuration loading.
"""

import json
import os
import yaml
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union
from pathlib import Path

from .distributed import DistributedConfig


# Environment-specific epochs for different deployment stages
ENVIRONMENT_EPOCHS = {
    "development": 1723323246031,  # Current default from original code
    "staging": 1723323246031,  # Same as dev for consistency during migration
    "production": 1420070400000,  # 2015-01-01 UTC (stable production epoch)
    "test": 1577836800000,  # 2020-01-01 UTC (for testing)
}

# Environment-specific defaults optimized for different use cases
ENVIRONMENT_DEFAULTS = {
    "development": {
        "time_bits": 39,
        "node_bits": 7,
        "worker_bits": 5,
        "auto_discover_node_id": False,  # Manual assignment in dev
        "auto_discover_worker_id": False,
    },
    "staging": {
        "time_bits": 41,  # Longer timestamp duration
        "node_bits": 5,  # Fewer nodes in staging
        "worker_bits": 5,
        "auto_discover_node_id": True,
        "auto_discover_worker_id": True,
    },
    "production": {
        "time_bits": 41,  # Longer timestamp duration for production
        "node_bits": 5,  # Support up to 32 nodes
        "worker_bits": 5,  # Support up to 32 workers per node
        "auto_discover_node_id": True,
        "auto_discover_worker_id": True,
    },
    "test": {
        "time_bits": 39,
        "node_bits": 7,
        "worker_bits": 5,
        "auto_discover_node_id": False,
        "auto_discover_worker_id": False,
    },
}


@dataclass(frozen=True)
class EnhancedSnowflakeIDConfig:
    """
    Enhanced Snowflake ID configuration with distributed coordination support.

    Extends the original SnowflakeIDConfig with environment awareness and
    distributed coordination capabilities for microservice deployments.
    """

    # Core Snowflake configuration (from original SnowflakeIDConfig)
    epoch: Optional[int] = None
    total_bits: int = 64
    time_bits: int = 39
    node_bits: int = 7
    worker_bits: int = 5
    node_id: int = 0
    worker_id: int = 0
    sequence_bits: Optional[int] = None  # Calculated automatically

    # Enhanced features
    environment: Optional[str] = None
    auto_discover_node_id: bool = False
    auto_discover_worker_id: bool = False
    distributed_config: Optional[DistributedConfig] = None
    fallback_strategy: str = "local_cache"  # local_cache, random, fail

    # Performance tuning
    enable_batch_generation: bool = True
    batch_size: int = 100
    pool_size: int = 1000

    # Monitoring and reliability
    enable_metrics: bool = True
    enable_health_checks: bool = True
    graceful_shutdown_timeout: int = 30

    def __post_init__(self):
        """
        Calculate sequence_bits and validate the configuration after initialization.
        """
        # Calculate sequence bits automatically
        calculated_sequence_bits = (
            self.total_bits - self.time_bits - self.node_bits - self.worker_bits
        )
        object.__setattr__(self, "sequence_bits", calculated_sequence_bits)

        # Apply environment-specific defaults if environment is set
        if self.environment and hasattr(self, "_apply_environment_defaults"):
            self._apply_environment_defaults()

        # Validate configuration
        self._validate_config()

    def _validate_config(self):
        """
        Validates the enhanced Snowflake ID configuration settings.

        Raises:
            ValueError: If any configuration setting is invalid.
        """
        # Validate core bit allocations
        if not isinstance(self.time_bits, int) or self.time_bits <= 0:
            raise ValueError("time_bits must be a positive integer.")
        if not isinstance(self.node_bits, int) or self.node_bits <= 0:
            raise ValueError("node_bits must be a positive integer.")
        if not isinstance(self.worker_bits, int) or self.worker_bits <= 0:
            raise ValueError("worker_bits must be a positive integer.")
        if not isinstance(self.sequence_bits, int) or self.sequence_bits <= 0:
            raise ValueError(
                "sequence_bits must be a positive integer (derived from other bit allocations)."
            )

        # Validate node_id range
        max_node_id = (1 << self.node_bits) - 1
        if not (0 <= self.node_id <= max_node_id):
            raise ValueError(
                f"Node ID ({self.node_id}) must be between 0 and {max_node_id} (inclusive)."
            )

        # Validate worker_id range
        max_worker_id = (1 << self.worker_bits) - 1
        if not (0 <= self.worker_id <= max_worker_id):
            raise ValueError(
                f"Worker ID ({self.worker_id}) must be between 0 and {max_worker_id} (inclusive)."
            )

        # Validate total bits allocation
        expected_total_bits = (
            self.time_bits + self.node_bits + self.worker_bits + self.sequence_bits
        )
        if self.total_bits != expected_total_bits:
            raise ValueError(
                f"The sum of time_bits, node_bits, worker_bits, and sequence_bits ({expected_total_bits}) "
                f"must equal total_bits ({self.total_bits})."
            )

        # Validate epoch
        current_epoch = (
            self.epoch if self.epoch is not None else self._get_default_epoch()
        )
        if not isinstance(current_epoch, int) or current_epoch <= 0:
            raise ValueError(
                "Epoch must be a positive integer representing milliseconds."
            )

        # Validate performance settings
        if self.batch_size <= 0 or self.batch_size > 10000:
            raise ValueError("batch_size must be between 1 and 10000.")
        if self.pool_size <= 0 or self.pool_size > 100000:
            raise ValueError("pool_size must be between 1 and 100000.")

        # Validate distributed configuration if auto-discovery is enabled
        if (
            self.auto_discover_node_id or self.auto_discover_worker_id
        ) and not self.distributed_config:
            raise ValueError(
                "distributed_config is required when auto_discover_node_id or auto_discover_worker_id is enabled."
            )

        # Validate fallback strategy
        valid_strategies = ["local_cache", "random", "fail"]
        if self.fallback_strategy not in valid_strategies:
            raise ValueError(f"fallback_strategy must be one of: {valid_strategies}")

    def _get_default_epoch(self) -> int:
        """Get the default epoch based on environment."""
        if self.environment and self.environment in ENVIRONMENT_EPOCHS:
            return ENVIRONMENT_EPOCHS[self.environment]
        return ENVIRONMENT_EPOCHS["development"]

    @classmethod
    def for_environment(
        cls, env: str = None, **overrides
    ) -> "EnhancedSnowflakeIDConfig":
        """
        Create configuration optimized for a specific environment.

        Args:
            env: Environment name (development, staging, production, test)
            **overrides: Additional configuration overrides

        Returns:
            EnhancedSnowflakeIDConfig: Environment-specific configuration
        """
        env = env or os.getenv("SNOWFLAKE_ENV", "development")

        # Get environment defaults
        base_config = ENVIRONMENT_DEFAULTS.get(
            env, ENVIRONMENT_DEFAULTS["development"]
        ).copy()

        # Set epoch
        base_config["epoch"] = ENVIRONMENT_EPOCHS.get(
            env, ENVIRONMENT_EPOCHS["development"]
        )
        base_config["environment"] = env

        # Apply overrides
        base_config.update(overrides)

        return cls(**base_config)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary for serialization."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, DistributedConfig):
                result[key] = {
                    "backend_type": value.backend_type,
                    "connection_params": value.connection_params,
                    "node_id_ttl": value.node_id_ttl,
                    "worker_pool_size": value.worker_pool_size,
                    "heartbeat_interval": value.heartbeat_interval,
                    "registration_timeout": value.registration_timeout,
                    "max_nodes": value.max_nodes,
                    "max_workers_per_node": value.max_workers_per_node,
                }
            else:
                result[key] = value
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EnhancedSnowflakeIDConfig":
        """Create configuration from dictionary."""
        config_data = data.copy()

        # Handle distributed_config if present
        if "distributed_config" in config_data and config_data["distributed_config"]:
            dist_config = config_data["distributed_config"]
            config_data["distributed_config"] = DistributedConfig(**dist_config)

        return cls(**config_data)


class ConfigurationLoader:
    """Supports loading configuration from various sources."""

    @staticmethod
    def from_file(path: Union[str, Path]) -> EnhancedSnowflakeIDConfig:
        """
        Load configuration from YAML or JSON file.

        Args:
            path: Path to configuration file

        Returns:
            EnhancedSnowflakeIDConfig: Loaded configuration

        Raises:
            FileNotFoundError: If configuration file doesn't exist
            ValueError: If file format is unsupported or invalid
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            if path.suffix.lower() in [".yaml", ".yml"]:
                try:
                    data = yaml.safe_load(f)
                except ImportError:
                    raise ImportError(
                        "PyYAML is required for YAML configuration files. Install with: pip install PyYAML"
                    )
            elif path.suffix.lower() == ".json":
                data = json.load(f)
            else:
                raise ValueError(
                    f"Unsupported file format: {path.suffix}. Use .yaml, .yml, or .json"
                )

        return EnhancedSnowflakeIDConfig.from_dict(data)

    @staticmethod
    def from_environment_variables(
        prefix: str = "SNOWFLAKE_",
    ) -> EnhancedSnowflakeIDConfig:
        """
        Load configuration from environment variables.

        Args:
            prefix: Prefix for environment variables (default: SNOWFLAKE_)

        Returns:
            EnhancedSnowflakeIDConfig: Configuration from environment
        """
        config_dict = {}

        # Define type conversions for different fields
        type_conversions = {
            "epoch": int,
            "total_bits": int,
            "time_bits": int,
            "node_bits": int,
            "worker_bits": int,
            "node_id": int,
            "worker_id": int,
            "sequence_bits": int,
            "auto_discover_node_id": lambda x: x.lower() in ["true", "1", "yes"],
            "auto_discover_worker_id": lambda x: x.lower() in ["true", "1", "yes"],
            "enable_batch_generation": lambda x: x.lower() in ["true", "1", "yes"],
            "batch_size": int,
            "pool_size": int,
            "enable_metrics": lambda x: x.lower() in ["true", "1", "yes"],
            "enable_health_checks": lambda x: x.lower() in ["true", "1", "yes"],
            "graceful_shutdown_timeout": int,
        }

        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix) :].lower()

                # Convert value to appropriate type
                if config_key in type_conversions:
                    try:
                        converted_value = type_conversions[config_key](value)
                        config_dict[config_key] = converted_value
                    except (ValueError, TypeError):
                        # Skip invalid values
                        continue
                else:
                    config_dict[config_key] = value

        # Handle distributed configuration from environment
        if any(key.startswith(f"{prefix}DISTRIBUTED_") for key in os.environ):
            dist_config = {}
            for key, value in os.environ.items():
                if key.startswith(f"{prefix}DISTRIBUTED_"):
                    dist_key = key[len(f"{prefix}DISTRIBUTED_") :].lower()
                    if dist_key == "connection_params":
                        # Parse JSON string for connection params
                        try:
                            dist_config[dist_key] = json.loads(value)
                        except json.JSONDecodeError:
                            # Fallback to simple key-value parsing
                            dist_config[dist_key] = {"url": value}
                    else:
                        dist_config[dist_key] = value

            if dist_config:
                config_dict["distributed_config"] = DistributedConfig(**dist_config)

        return EnhancedSnowflakeIDConfig(**config_dict)

    @staticmethod
    async def from_consul(consul_client, key: str) -> EnhancedSnowflakeIDConfig:
        """
        Load configuration from Consul KV store.

        Args:
            consul_client: Consul client instance
            key: Consul key path for configuration

        Returns:
            EnhancedSnowflakeIDConfig: Configuration from Consul

        Raises:
            ValueError: If configuration not found or invalid
        """
        try:
            index, data = await consul_client.kv.get(key)
            if data is None:
                raise ValueError(f"Configuration not found at Consul key: {key}")

            config_data = json.loads(data["Value"].decode())
            return EnhancedSnowflakeIDConfig.from_dict(config_data)
        except Exception as e:
            raise ValueError(f"Failed to load configuration from Consul: {e}")

    @staticmethod
    def get_default_config_path() -> Optional[Path]:
        """
        Get the default configuration file path based on environment.

        Returns:
            Path to default configuration file or None if not found
        """
        env = os.getenv("SNOWFLAKE_ENV", "development")

        # Check common configuration file locations
        config_paths = [
            Path(f"config/snowflake-{env}.yaml"),
            Path(f"config/snowflake-{env}.yml"),
            Path(f"config/snowflake-{env}.json"),
            Path("config/snowflake.yaml"),
            Path("config/snowflake.yml"),
            Path("config/snowflake.json"),
            Path(f"snowflake-{env}.yaml"),
            Path(f"snowflake-{env}.yml"),
            Path(f"snowflake-{env}.json"),
            Path("snowflake.yaml"),
            Path("snowflake.yml"),
            Path("snowflake.json"),
        ]

        for path in config_paths:
            if path.exists():
                return path

        return None

    @classmethod
    def load_config(
        cls,
        config_path: Optional[Union[str, Path]] = None,
        environment: Optional[str] = None,
        **overrides,
    ) -> EnhancedSnowflakeIDConfig:
        """
        Load configuration using the best available method.

        Priority order:
        1. Explicit config file path
        2. Environment variables
        3. Default config file locations
        4. Environment-specific defaults

        Args:
            config_path: Explicit path to configuration file
            environment: Environment name for defaults
            **overrides: Configuration overrides

        Returns:
            EnhancedSnowflakeIDConfig: Loaded configuration
        """
        # Try explicit config file path
        if config_path:
            try:
                config = cls.from_file(config_path)
                # Apply overrides
                if overrides:
                    config_dict = config.to_dict()
                    config_dict.update(overrides)
                    config = EnhancedSnowflakeIDConfig.from_dict(config_dict)
                return config
            except (FileNotFoundError, ValueError) as e:
                raise ValueError(
                    f"Failed to load configuration from {config_path}: {e}"
                )

        # Try environment variables
        try:
            config = cls.from_environment_variables()
            if overrides:
                config_dict = config.to_dict()
                config_dict.update(overrides)
                config = EnhancedSnowflakeIDConfig.from_dict(config_dict)
            return config
        except Exception:
            pass  # Fall back to other methods

        # Try default config file locations
        default_path = cls.get_default_config_path()
        if default_path:
            try:
                config = cls.from_file(default_path)
                if overrides:
                    config_dict = config.to_dict()
                    config_dict.update(overrides)
                    config = EnhancedSnowflakeIDConfig.from_dict(config_dict)
                return config
            except Exception:
                pass  # Fall back to environment defaults

        # Fall back to environment-specific defaults
        return EnhancedSnowflakeIDConfig.for_environment(environment, **overrides)

"""
Distributed coordination components for Snowflake ID generation.

This module provides the infrastructure for coordinating node and worker IDs
across multiple instances in a distributed microservice environment.
"""

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DistributedConfig:
    """Configuration for distributed coordination backend."""

    backend_type: str = "redis"  # redis, etcd, consul, memory
    connection_params: Dict[str, Any] = field(default_factory=dict)
    node_id_ttl: int = 300  # seconds
    worker_pool_size: int = 32
    heartbeat_interval: int = 30  # seconds
    registration_timeout: int = 10  # seconds
    max_nodes: int = 32  # Maximum number of nodes (2^5 for 5-bit node_id)
    max_workers_per_node: int = 32  # Maximum workers per node


class NodeRegistry(Protocol):
    """Protocol for distributed node coordination backends."""

    async def register_node(self, node_id: int, metadata: Dict[str, Any]) -> bool:
        """Register a node with the distributed registry."""
        ...

    async def discover_available_node_id(self) -> int:
        """Find and claim an available node ID."""
        ...

    async def register_worker(self, node_id: int, worker_id: int) -> bool:
        """Register a worker within a node."""
        ...

    async def heartbeat(self, node_id: int, worker_id: int) -> bool:
        """Send heartbeat to maintain registration."""
        ...

    async def unregister_node(self, node_id: int) -> bool:
        """Unregister a node from the registry."""
        ...

    async def cleanup_expired_registrations(self) -> int:
        """Clean up expired node/worker registrations."""
        ...


class MemoryNodeRegistry:
    """In-memory implementation of NodeRegistry for testing and single-instance use."""

    def __init__(self, config: DistributedConfig):
        self.config = config
        self.nodes: Dict[int, Dict[str, Any]] = {}
        self.workers: Dict[
            tuple, Dict[str, Any]
        ] = {}  # (node_id, worker_id) -> metadata
        self._lock = asyncio.Lock()

    async def register_node(self, node_id: int, metadata: Dict[str, Any]) -> bool:
        """Register a node with timestamp."""
        async with self._lock:
            if node_id in self.nodes:
                return False  # Node already registered

            self.nodes[node_id] = {
                **metadata,
                "registered_at": time.time(),
                "last_heartbeat": time.time(),
            }
            logger.info(f"Registered node {node_id}")
            return True

    async def discover_available_node_id(self) -> int:
        """Find the first available node ID."""
        async with self._lock:
            # Clean up expired nodes first
            await self._cleanup_expired_nodes()

            for node_id in range(self.config.max_nodes):
                if node_id not in self.nodes:
                    # Try to register this node immediately
                    metadata = {
                        "discovered_at": datetime.utcnow().isoformat(),
                        "instance_id": f"node_{node_id}_{int(time.time())}",
                    }
                    if await self.register_node(node_id, metadata):
                        return node_id

            raise RuntimeError("No available node IDs")

    async def register_worker(self, node_id: int, worker_id: int) -> bool:
        """Register a worker within a node."""
        async with self._lock:
            if node_id not in self.nodes:
                return False  # Node must be registered first

            worker_key = (node_id, worker_id)
            if worker_key in self.workers:
                return False  # Worker already registered

            self.workers[worker_key] = {
                "registered_at": time.time(),
                "last_heartbeat": time.time(),
            }
            logger.info(f"Registered worker {worker_id} on node {node_id}")
            return True

    async def heartbeat(self, node_id: int, worker_id: int) -> bool:
        """Update heartbeat timestamp."""
        async with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id]["last_heartbeat"] = time.time()

            worker_key = (node_id, worker_id)
            if worker_key in self.workers:
                self.workers[worker_key]["last_heartbeat"] = time.time()
                return True

            return False

    async def unregister_node(self, node_id: int) -> bool:
        """Unregister a node and its workers."""
        async with self._lock:
            if node_id not in self.nodes:
                return False

            # Remove the node
            del self.nodes[node_id]

            # Remove all workers for this node
            workers_to_remove = [
                key for key in self.workers.keys() if key[0] == node_id
            ]
            for worker_key in workers_to_remove:
                del self.workers[worker_key]

            logger.info(
                f"Unregistered node {node_id} and {len(workers_to_remove)} workers"
            )
            return True

    async def cleanup_expired_registrations(self) -> int:
        """Clean up expired registrations."""
        async with self._lock:
            current_time = time.time()
            expired_count = 0

            # Clean up expired nodes
            expired_nodes = [
                node_id
                for node_id, metadata in self.nodes.items()
                if current_time - metadata.get("last_heartbeat", 0)
                > self.config.node_id_ttl
            ]

            for node_id in expired_nodes:
                await self.unregister_node(node_id)
                expired_count += 1

            return expired_count

    async def _cleanup_expired_nodes(self):
        """Internal method to clean up expired nodes."""
        await self.cleanup_expired_registrations()


class RedisNodeRegistry:
    """Redis-based implementation of NodeRegistry for production use."""

    def __init__(self, config: DistributedConfig):
        self.config = config
        self.redis_client = None
        self._connection_params = config.connection_params

    async def _get_redis_client(self):
        """Get or create Redis client."""
        if self.redis_client is None:
            try:
                import aioredis

                self.redis_client = aioredis.from_url(
                    self._connection_params.get("url", "redis://localhost:6379"),
                    **{k: v for k, v in self._connection_params.items() if k != "url"},
                )
            except ImportError:
                raise ImportError(
                    "aioredis is required for Redis backend. Install with: pip install aioredis"
                )

        return self.redis_client

    async def register_node(self, node_id: int, metadata: Dict[str, Any]) -> bool:
        """Register a node using Redis SETNX with TTL."""
        redis = await self._get_redis_client()
        node_key = f"snowflake:nodes:{node_id}"

        # Use SETNX (SET if Not eXists) for atomic registration
        registration_data = {
            **metadata,
            "registered_at": time.time(),
            "last_heartbeat": time.time(),
        }

        # Try to set the key only if it doesn't exist
        success = await redis.set(
            node_key,
            json.dumps(registration_data),
            nx=True,  # Only set if key doesn't exist
            ex=self.config.node_id_ttl,  # Set TTL
        )

        if success:
            logger.info(f"Registered node {node_id} in Redis")
            return True
        return False

    async def discover_available_node_id(self) -> int:
        """Find an available node ID using Redis."""
        redis = await self._get_redis_client()

        # Clean up expired registrations first
        await self.cleanup_expired_registrations()

        for node_id in range(self.config.max_nodes):
            node_key = f"snowflake:nodes:{node_id}"
            exists = await redis.exists(node_key)

            if not exists:
                # Try to register this node
                metadata = {
                    "discovered_at": datetime.utcnow().isoformat(),
                    "instance_id": f"node_{node_id}_{int(time.time())}",
                }
                if await self.register_node(node_id, metadata):
                    return node_id

        raise RuntimeError("No available node IDs")

    async def register_worker(self, node_id: int, worker_id: int) -> bool:
        """Register a worker in Redis."""
        redis = await self._get_redis_client()
        node_key = f"snowflake:nodes:{node_id}"
        worker_key = f"snowflake:workers:{node_id}:{worker_id}"

        # Check if node exists
        node_exists = await redis.exists(node_key)
        if not node_exists:
            return False

        # Register worker with TTL
        worker_data = {
            "registered_at": time.time(),
            "last_heartbeat": time.time(),
            "node_id": node_id,
        }

        success = await redis.set(
            worker_key, json.dumps(worker_data), nx=True, ex=self.config.node_id_ttl
        )

        if success:
            logger.info(f"Registered worker {worker_id} on node {node_id} in Redis")
            return True
        return False

    async def heartbeat(self, node_id: int, worker_id: int) -> bool:
        """Send heartbeat to refresh TTLs."""
        redis = await self._get_redis_client()

        node_key = f"snowflake:nodes:{node_id}"
        worker_key = f"snowflake:workers:{node_id}:{worker_id}"

        # Update heartbeat timestamps and refresh TTLs
        pipe = redis.pipeline()

        # Update node heartbeat
        node_data = await redis.get(node_key)
        if node_data:
            node_info = json.loads(node_data)
            node_info["last_heartbeat"] = time.time()
            pipe.set(node_key, json.dumps(node_info), ex=self.config.node_id_ttl)

        # Update worker heartbeat
        worker_data = await redis.get(worker_key)
        if worker_data:
            worker_info = json.loads(worker_data)
            worker_info["last_heartbeat"] = time.time()
            pipe.set(worker_key, json.dumps(worker_info), ex=self.config.node_id_ttl)

        results = await pipe.execute()
        return all(results)

    async def unregister_node(self, node_id: int) -> bool:
        """Unregister node and its workers from Redis."""
        redis = await self._get_redis_client()

        # Delete node key
        node_key = f"snowflake:nodes:{node_id}"
        await redis.delete(node_key)

        # Delete all worker keys for this node
        worker_pattern = f"snowflake:workers:{node_id}:*"
        worker_keys = await redis.keys(worker_pattern)
        if worker_keys:
            await redis.delete(*worker_keys)

        logger.info(
            f"Unregistered node {node_id} and {len(worker_keys)} workers from Redis"
        )
        return True

    async def cleanup_expired_registrations(self) -> int:
        """Redis TTL handles expiration automatically, but we can clean up manually."""
        redis = await self._get_redis_client()

        # Get all node keys
        node_keys = await redis.keys("snowflake:nodes:*")
        expired_count = 0

        for node_key in node_keys:
            ttl = await redis.ttl(node_key)
            if ttl == -1:  # Key exists but has no TTL (shouldn't happen)
                await redis.expire(node_key, self.config.node_id_ttl)
            elif ttl == -2:  # Key doesn't exist (expired)
                expired_count += 1

        return expired_count


def create_node_registry(config: DistributedConfig) -> NodeRegistry:
    """Factory function to create appropriate NodeRegistry implementation."""
    if config.backend_type.lower() == "redis":
        return RedisNodeRegistry(config)
    elif config.backend_type.lower() == "memory":
        return MemoryNodeRegistry(config)
    else:
        raise ValueError(f"Unsupported backend type: {config.backend_type}")


class HeartbeatManager:
    """Manages periodic heartbeats to maintain node/worker registrations."""

    def __init__(self, registry: NodeRegistry, config: DistributedConfig):
        self.registry = registry
        self.config = config
        self.node_id: Optional[int] = None
        self.worker_id: Optional[int] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False

    def start_heartbeat(self, node_id: int, worker_id: int):
        """Start the heartbeat process."""
        self.node_id = node_id
        self.worker_id = worker_id
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"Started heartbeat for node {node_id}, worker {worker_id}")

    def stop_heartbeat(self):
        """Stop the heartbeat process."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            logger.info("Stopped heartbeat")

    async def _heartbeat_loop(self):
        """Main heartbeat loop."""
        while self._running:
            try:
                if self.node_id is not None and self.worker_id is not None:
                    success = await self.registry.heartbeat(
                        self.node_id, self.worker_id
                    )
                    if not success:
                        logger.warning(
                            f"Heartbeat failed for node {self.node_id}, worker {self.worker_id}"
                        )

                await asyncio.sleep(self.config.heartbeat_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(self.config.heartbeat_interval)

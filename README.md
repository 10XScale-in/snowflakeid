# Snowflake ID Generator

A Python library for generating unique, distributed IDs using a modified Snowflake algorithm. This library allows for easy generation of Snowflake IDs, encoding/decoding to Base62, and extracting information from the generated IDs.

## Features

- **Asynchronous ID generation:** Supports high-throughput applications.
- **Customizable bit allocation:** Configurable time, node, worker, and sequence bits.
- **Base62 encoding/decoding:** Easily encode Snowflake IDs into a compact, URL-friendly format.
- **Extractable components:** Retrieve timestamp, node ID, worker ID, and sequence number from a generated Snowflake ID.

## Installation

You can install the library using pip after building the package:

```bash
pip install git+https://github.com/10XScale-in/snowflakeid.git
```

or
```bash
pip install https://github.com/10XScale-in/snowflakeid/releases/download/v0.1.0/snowflakeid-0.1.0-py3-none-any.whl
```

## Usage

### Basic Usage

Here's a quick example of how to use the Snowflake ID generator:

```python
import asyncio
from snowflakeid import SnowflakeIDGenerator, SnowflakeIDConfig

async def main():
    generator = SnowflakeIDGenerator()
    snowflake_id = await generator.generate()
    print(f"Generated Snowflake ID: {snowflake_id}")

    # Base62 encoding
    encoded_id = SnowflakeIDGenerator.encode_base62(snowflake_id)
    print(f"Base62 Encoded ID: {encoded_id}")

    # Decoding back to Snowflake ID
    decoded_id = SnowflakeIDGenerator.decode_base62(encoded_id)
    print(f"Decoded Snowflake ID: {decoded_id}")

    # Extracting components from Snowflake ID
    components = generator.extract_snowflake_info(snowflake_id)
    print(f"Extracted Components: {components}")

asyncio.run(main())
```

### Configuration

You can customize the ID generation by passing a `SnowflakeIDConfig` object to the `SnowflakeIDGenerator`. All configuration parameters are adjustable, allowing you to tailor the generator to your specific needs.

#### Custom Configuration Example (64-bit ID)

```python
from snowflakeid import SnowflakeIDGenerator, SnowflakeIDConfig

config = SnowflakeIDConfig(
    epoch=1609459200000,  # Custom epoch (January 1, 2021)
    node_id=1,
    worker_id=2,
    time_bits=39,   # 39 bits for time
    node_bits=5,    # 5 bits for node ID (up to 32 nodes)
    worker_bits=8   # 8 bits for worker ID (up to 256 workers)
)
generator = SnowflakeIDGenerator(config=config)
```

### Custom Configuration Examples for Different Bit Allocations

#### Example 1: 64-bit ID

- **Time Bits (39 bits):** Encodes the timestamp relative to the epoch, allowing up to ~17 years of milliseconds.
- **Node Bits (5 bits):** Allows for up to 32 unique nodes.
- **Worker Bits (8 bits):** Allows for up to 256 unique workers per node.
- **Sequence Bits (12 bits):** Automatically calculated to allow up to 4096 IDs to be generated per worker, per millisecond.

```python
from snowflakeid import SnowflakeIDGenerator, SnowflakeIDConfig

config = SnowflakeIDConfig(
    epoch=1609459200000,
    node_id=1,
    worker_id=2,
    time_bits=39,
    node_bits=5,
    worker_bits=8
)
generator = SnowflakeIDGenerator(config=config)
```

#### Example 2: 32-bit ID

- **Time Bits (21 bits):** Encodes the timestamp relative to the epoch, allowing up to ~2 minutes of milliseconds.
- **Node Bits (1 bit):** Allows for 2 unique nodes.
- **Worker Bits (6 bits):** Allows for up to 64 unique workers per node.
- **Sequence Bits (4 bits):** Automatically calculated to allow up to 16 IDs to be generated per worker, per millisecond.

```python
from snowflakeid import SnowflakeIDGenerator, SnowflakeIDConfig

config = SnowflakeIDConfig(
    epoch=1609459200000,
    node_id=1,
    worker_id=2,
    time_bits=21,
    node_bits=1,
    worker_bits=6,
    total_bits=32  # Adjust total bits to 32 for a 32-bit ID
)
generator = SnowflakeIDGenerator(config=config)
```

### Default Bit Allocation

By default, the Snowflake ID is a 64-bit integer with the following bit allocation:

- **Total Bits (64 bits):** The total number of bits used to generate the ID.
- **Time Bits (40 bits):** Encodes the timestamp relative to the specified epoch.
- **Node Bits (7 bits):** Identifies the node generating the ID.
- **Worker Bits (5 bits):** Identifies the worker within the node.
- **Sequence Bits (12 bits):** A sequence counter that ensures uniqueness within the same millisecond.

### Extracting Snowflake ID Components

You can extract individual components (timestamp, node ID, worker ID, and sequence) from a Snowflake ID:

```python
from snowflakeid import SnowflakeIDGenerator, SnowflakeIDConfig
generator = SnowflakeIDGenerator()
snowflake_id = await generator.generate()
components = generator.extract_snowflake_info(snowflake_id)
print(components)
```

This will return a SnowflakeIDInfo with the following attributes:
- `datetime`: The time at which the ID was generated, datetime class.
- `node_id`: The ID of the node that generated the ID.
- `worker_id`: The ID of the worker that generated the ID.
- `sequence`: The sequence number within the same millisecond.

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

## Testing

To run the unit tests, use the following command:

```bash
pytest
```

## Acknowledgements
This project is inspired by Twitter's [Snowflake ID generation algorithm](https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake). Special thanks to the contributors and open-source community.

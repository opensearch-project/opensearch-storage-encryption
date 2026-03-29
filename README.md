# Opensearch Storage Encryption

A high-performance OpenSearch plugin that provides transparent, on-the-fly encryption and decryption of index data at rest. This plugin implements encryption at the Lucene Directory level, offering a seamless integration that requires no changes to application code. Currently only supporting AWS KMS as master key provider but will allow other key providers as well in future versions.

## Features

The plugin provides transparent on-the-fly encryption and decryption at the Lucene Directory level with minimal performance overhead. It offers multiple directory implementations including standard NIO FS for filesystem-based encryption, Direct I/O with io_uring for Linux-optimized high-performance I/O, and Hybrid Directory that combines encrypted and unencrypted storage for flexibility.

Performance is optimized through multiple mechanisms: a Caffeine-based block cache for rapid read operations, intelligent read-ahead prefetching for sequential access patterns, and efficient memory segment pooling for encryption operations. Native cipher support via OpenSSL JNI integration enables hardware-accelerated encryption.

Security features include support for both AES-CTR and AES-GCM encryption algorithms, along with advanced key management capabilities. The key management system uses HKDF-based key derivation, implements node-level and shard-level key caching, and continuously monitors master key health. Complete data protection extends to transaction logs through translog encryption.

Built-in monitoring and comprehensive metrics provide visibility into plugin operations and performance characteristics.

## Architecture

```
┌──────────────┐
│  Tenant A    │
│ (plain text) │────────┐
└──────────────┘        │
                        │
┌──────────────┐        │         ┌─────────────────────────────────────────┐
│  Tenant B    │        │         │       OpenSearch Node                   │
│ (encrypted)  │────────┼────────►│                                         │
└──────────────┘        │         │  ┌────────────────────────────────┐     │
                        │         │  │   Query Processing Layer       │     │
                        └────────►│  │   (Lucene Directory API)       │     │
                                  │  └─────────────┬──────────────────┘     │
                                  │                │                        │
                                  │    ┌───────────┴──────────┐             │
                                  │    │                      │             │
                                  │    ▼                      ▼             │
                                  │  ┌──────────────┐   ┌──────────────┐    │
                                  │  │   Standard   │   │    Crypto    │    │
                                  │  │  Directory   │   │  Directory   │    │
                                  │  │              │   │      🔐      │    │
                                  │  └──────┬───────┘   └──────┬───────┘    │
                                  │         │                  │            │
                                  └─────────┼──────────────────┼────────────┘
                                            │                  │
                                            │                  │ Encrypt/Decrypt
                                            │                  │ ┌─────────────┐
                                            ▼                  ├─┤ Block Cache │
                                     ┌─────────────┐           │ └─────────────┘
                                     │   Disk      │           │
                                     │             │           ▼
                                     │ Plain Text  │    ┌─────────────┐
                                     │   Shards    │    │   Disk      │
                                     │  (Tenant A) │    │             │
                                     └─────────────┘    │ 🔒 Encrypted│
                                                        │   Shards    │
                                                        │  (Tenant B) │
                                                        └──────┬──────┘
                                                               │
                                                               │ Key Operations
                                                               ▼
                                                        ┌─────────────────────────┐
                                                        │   Master Key Provider   │
                                                        │        🔑 Master        │
                                                        │           Keys          │
                                                        └─────────────────────────┘

Legend:  🔐 Encryption Point    🔒 Encrypted Storage    🔑 Key Management
```

**Multi-Tenant Encryption:**
- **Tenant A**: Uses standard directory → data stored in plain text
- **Tenant B**: Uses crypto directory → data encrypted at rest
- Both tenants query using plain text → encryption is transparent

### Key Components

#### CryptoDirectory Layer
The plugin implements custom Lucene Directory implementations that intercept all file I/O operations. `CryptoNIOFSDirectory` provides standard NIO-based encrypted filesystem operations, while `CryptoDirectIODirectory` offers Linux-optimized Direct I/O with io_uring support for high-throughput scenarios. `HybridCryptoDirectory` provides flexibility by selectively encrypting files based on patterns.

#### Block Cache
A high-performance caching layer built on the Caffeine library provides optimized read operations. The cache features configurable size and eviction policies with block-level granularity for optimal memory usage. Thread-safe concurrent access is supported through an LRU eviction strategy.

#### Key Management
The plugin implements a multi-tier key resolution and caching architecture. `NodeLevelKeyCache` maintains a cluster-wide cache for shared keys, while `ShardKeyResolverRegistry` handles per-shard key resolution. `DefaultKeyResolver` performs HKDF-based key derivation from master keys, and `MasterKeyHealthMonitor` continuously monitors master key availability.

#### Encryption Engine
The encryption engine supports multiple cipher implementations. `AesCipherFactory` provides AES-CTR mode encryption, while `AesGcmCipherFactory` offers AES-GCM mode with authentication. `OpenSslNativeCipher` enables hardware-accelerated native encryption via OpenSSL integration.

#### Memory Management
Efficient memory management is achieved through `MemorySegmentPool`, which provides pooling of off-heap memory segments. `RefCountedMemorySegment` ensures safe concurrent access through reference counting. The system supports configurable pool sizes and warmup strategies to optimize resource utilization.

#### I/O Optimization
I/O performance is enhanced through several optimization mechanisms. `ReadaheadManager` provides intelligent prefetching for sequential reads, while `BlockLoader` enables async block loading with io_uring on Linux systems. `DirectIOReaderUtil` offers utilities for bypassing the OS page cache when using Direct I/O.

## Requirements

- **JDK**: 25
- **OpenSearch**: 3.6.0-SNAPSHOT or compatible version
- **Master Key Provider**: Required for key management
- **Operating System**: Linux recommended for Direct I/O features (io_uring)

## Installation

### Build from Source

```bash
# Clone the repository
git clone https://github.com/opensearch-project/opensearch-storage-encryption.git
cd opensearch-storage-encryption

# Build the plugin
./gradlew clean assemble

# The plugin zip will be located at:
# build/distributions/storage-encryption.zip
```

### Install Plugin

```bash
# Install using opensearch-plugin command
cd /path/to/opensearch
bin/opensearch-plugin install file:///path/to/storage-encryption.zip
```

### Configure KMS

Add KMS configuration to `opensearch.yml`:

```yaml
kms.region: us-east-1
kms.key_arn: arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
```

Add AWS credentials to the keystore:

```bash
bin/opensearch-keystore create
echo "your-access-key" | bin/opensearch-keystore add -x kms.access_key
echo "your-secret-key" | bin/opensearch-keystore add -x kms.secret_key
echo "your-session-token" | bin/opensearch-keystore add -x kms.session_token
```

## Usage

### Creating an Encrypted Index

```json
PUT /encrypted-index
{
  "settings": {
    "index.store.type": "cryptofs",
    "index.store.crypto.kms.type": "aws-kms",
    "index.store.crypto.kms.key_arn": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
  }
}
```

### Index Template Example

```json
PUT /_index_template/encrypted_template
{
  "index_patterns": ["logs-*"],
  "template": {
    "settings": {
      "index.store.type": "cryptofs",
      "index.store.crypto.kms.type": "aws-kms",
      "index.store.crypto.kms.key_arn": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
    }
  }
}
```

## Configuration Reference

### Index-Level Settings

Configure these when creating an index. Setting `index.store.type` to `"cryptofs"` enables encryption.

| Setting | Description | Type | Options/Range |
|---------|-------------|------|---------------|
| `index.store.type` | **Set to `"cryptofs"` to enable encryption** | String | `cryptofs` (for encryption) or standard types (`niofs`, `hybridfs`, `mmapfs`, `simplefs`) |
| `index.store.crypto.provider` | Crypto provider for encryption | String | Any registered Java Security provider |
| `index.store.crypto.key_provider` | Key management provider type **(required when using cryptofs)** | String | `aws-kms`, `dummy` (testing only) |
| `index.store.crypto.kms.key_arn` | AWS KMS key ARN (master key) **(required when using cryptofs)** | String | Valid AWS KMS ARN format |
| `index.store.crypto.kms.encryption_context` | AWS KMS encryption context **(required when using cryptofs)** | String | Additional authenticated data for KMS |

**How Encryption Works:**
- When you set `index.store.type: "cryptofs"`, the plugin automatically wraps the node's default store type with encryption
- The actual encrypted directory implementation used depends on the node's configuration:
  - `NIOFS` → Uses `CryptoNIOFSDirectory` (standard NIO-based encryption)
  - `HYBRIDFS` → Uses `HybridCryptoDirectory` (Direct I/O + block caching on Linux)
  - `MMAPFS` → Uses `CryptoDirectIODirectory` (Direct I/O, as MMAP is not supported)
  - `SIMPLEFS` → Uses `CryptoNIOFSDirectory`

**Example:**
```json
{
  "settings": {
    "index.store.type": "cryptofs",
    "index.store.crypto.provider": "SunJCE",
    "index.store.crypto.key_provider": "aws-kms",
    "index.store.crypto.kms.key_arn": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
    "index.store.crypto.kms.encryption_context": "project=myapp"
  }
}
```

### Node-Level Settings

Configure in `opensearch.yml`:

| Setting | Description | Type | Range |
|---------|-------------|------|-------|
| `node.store.crypto.pool_size_percentage` | Memory pool size as percentage of off-heap memory| Double | `0.0` - `1.0` |
| `node.store.crypto.cache_to_pool_ratio` | Cache size as ratio of pool size (cache = pool × ratio) | Double | `0.1` - `1.0` |
| `node.store.crypto.warmup_percentage` | Percentage of cache blocks to warmup at initialization | Double | `0.0` - `1.0` |
| `node.store.crypto.key_refresh_interval` | Interval for refreshing data keys from KMS | TimeValue | `-1` (never) or positive duration |
| `node.store.crypto.key_expiry_interval` | Expiration time for keys after refresh failures | TimeValue | `-1` (never) or positive duration |

**Time Value Format:**
- Supported units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days)
- Examples: `30s`, `5m`, `1h`, `24h`
- Special value: `-1` disables refresh/expiry

**Memory Configuration Notes:**
- Pool size is calculated as: `(Total Physical Memory - Max Heap) × pool_size_percentage`
- Cache size is calculated as: `Pool Size × cache_to_pool_ratio`
- Minimum pool size: 256 MB

**Example Configuration:**
```yaml
# opensearch.yml
node.store.crypto.pool_size_percentage: 0.3
node.store.crypto.cache_to_pool_ratio: 0.75
node.store.crypto.warmup_percentage: 0.05
node.store.crypto.key_refresh_interval: 1h
node.store.crypto.key_expiry_interval: 24h
```

## Security Considerations

### Data Protection
- **Encryption at Rest**: All index data and translogs are encrypted using AES-256
- **Key Derivation**: Per-shard keys derived from master key using HKDF
- **Authentication**: AES-GCM mode provides authenticated encryption

### Key Management
- **Master Key**: Stored with Master Key Provider (example: AWS-KMS)
- **Data Keys**: Encrypted data keys stored in shard metadata
- **Key Health**: Continuous monitoring of master key availability

## Testing

Run the test suites:

```bash
# Unit tests
./gradlew test

# Integration tests
./gradlew internalClusterTest

# YAML REST tests
./gradlew yamlRestTest

# All tests
./gradlew allTests
```

## JMH Benchmarks

The project includes JMH microbenchmarks for measuring read throughput of the BufferPool (encrypted) directory vs MMap (plaintext) directory. Benchmark sources live under `src/jmh/java/org/opensearch/index/store/benchmark/`.

### Running Benchmarks

```bash
# Full run (defaults: 2 forks, 3 warmup iterations, 5 measurement iterations, 3s/5s durations)
./gradlew jmh

# Quick smoke test with overrides
./gradlew jmh -Pjmh.fork=1 -Pjmh.wi=1 -Pjmh.i=1 -Pjmh.w=1s -Pjmh.r=1s
```

All JMH parameters are overridable via `-P` flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-Pjmh.fork` | `2` | Number of forked JVM processes |
| `-Pjmh.wi` | `3` | Warmup iterations |
| `-Pjmh.i` | `5` | Measurement iterations |
| `-Pjmh.w` | `3s` | Warmup iteration duration |
| `-Pjmh.r` | `5s` | Measurement iteration duration |

Results are written to:
- `build/reports/jmh/human.txt` — human-readable output
- `build/reports/jmh/results.json` — JSON format for tooling

### Regenerating JMH Harness Classes

After adding, removing, or renaming any `@Benchmark` method, regenerate the JMH harness:

```bash
./gradlew jmhPrepare
```

### Running Benchmarks from Fat JAR

You can also build a self-contained fat JAR and run benchmarks directly:

```bash
# Build the fat JAR
./gradlew jmhJar

# List all available benchmarks
java --enable-preview --enable-native-access=ALL-UNNAMED \
  -jar build/distributions/storage-encryption-jmh.jar -l

# Run a specific benchmark with custom settings
java --enable-preview --enable-native-access=ALL-UNNAMED \
  -jar build/distributions/storage-encryption-jmh.jar RadixBlockTable -wi 2 -i 3 -f 1

# Run all benchmarks
java --enable-preview --enable-native-access=ALL-UNNAMED \
  -jar build/distributions/storage-encryption-jmh.jar
```

### Benchmark Classes

- `HotPathReadBenchmarks` — 16 benchmark methods measuring throughput with data pre-warmed in block cache (BufferPool) or page cache (MMap). Covers all `RandomAccessInput` and `IndexInput` read APIs plus a mixed workload that randomly interleaves all API types.
- `RadixBlockTableBenchmark` — 7 benchmark methods comparing L1/L2 cache lookup strategies: RadixBlockTable (two plain array loads), CaffeineBlockCache (ConcurrentHashMap), and stamp-gated cache (VarHandle acquire/release). Includes L1+L2 fallback paths and sparse directory growth scenarios.

### Filtering Benchmarks

Use `-Pjmh.includes` to run a specific benchmark method (regex match):

```bash
# Run only the sequential readByte benchmark
./gradlew jmh -Pjmh.includes='sequentialReadBytesFromClone'

# Run all random read benchmarks
./gradlew jmh -Pjmh.includes='randomRead.*'
```

### Key Parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `directoryType` | `bufferpool`, `mmap`, `multisegment_mmap_impl` | `bufferpool`: encrypted block cache + direct I/O; `mmap`: single contiguous mmap (Lucene default); `multisegment_mmap_impl`: mmap chunked at `CACHE_BLOCK_SIZE` boundaries |
| `fileSizeMB` | `32` | Size of each test file in MB |
| `threadCount` | `8` | Number of concurrent reader threads per benchmark invocation |
| `numFilesToRead` | `1` | Number of files read per invocation |

> **Note:** The `@Warmup`, `@Measurement`, and `@Fork` annotations in `HotPathReadBenchmarks` take precedence over `-Pjmh.*` command-line flags. To override them, edit the annotation values directly in the source.

## Limitations

- Currently only AWS KMS and linux are supported.
- Cluster metadata is not encrypted

## Contributing

We welcome contributions! Please see our [Developer Guide](DEVELOPER_GUIDE.md) for detailed information on contributing.

## Documentation

- [Developer Guide](DEVELOPER_GUIDE.md) - Comprehensive guide for developers
- [Maintainers](MAINTAINERS.md) - Project maintainers

## License

This project is licensed under the Apache License 2.0. See [LICENSE.txt](LICENSE.txt) for details.

## Support

For issues, questions, or contributions:
- **Issues**: [GitHub Issues](https://github.com/opensearch-project/opensearch-storage-encryption/issues)
- **Discussions**: [OpenSearch Forums](https://forum.opensearch.org/)
- **Security**: See [SECURITY.md](SECURITY.md) for reporting security vulnerabilities

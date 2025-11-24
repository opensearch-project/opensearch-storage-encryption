# Developer Guide

This guide helps developers get started with building and testing the OpenSearch Storage Encryption plugin.

## Prerequisites

1. **JDK 21** or higher (Note: Code uses preview features that are patched at build time)

2. Gradle (included via wrapper)

3. Linux with kernel 5.1+ (recommended for Direct I/O with io_uring support)

## Environment Setup

Create an environment file for your AWS credentials and KMS configuration:

```bash
# Create environment file for sensitive information
cat > .env << 'EOF'
# AWS Credentials for KMS
AWS_ACCESS_KEY_ID="your_access_key_here"
AWS_SECRET_ACCESS_KEY="your_secret_key_here"
AWS_SESSION_TOKEN="your_session_token_here"
# AWS KMS configuration
KMS_REGION="<your_kms_region>"
KMS_KEY_ARN="<your_kms_key_arn>"
EOF

# Edit the file with your actual values
# Then source the environment file
source .env
```

## Building the Plugin Alone

If you only need to build the plugin without setting up a full OpenSearch environment:

```bash
# Clone the Storage Encryption plugin
git clone https://github.com/opensearch-project/opensearch-storage-encryption.git
cd opensearch-storage-encryption

# Build the plugin
./gradlew clean assemble

# Run all checks including tests
./gradlew check
```

## Development Setup with OpenSearch

For a complete development environment:

```bash
# Set up required variables
OPENSEARCH_VERSION="3.3.0-SNAPSHOT"
BASE_DIR="$(pwd)"
OPENSEARCH_DIR="${BASE_DIR}/OpenSearch"
STORAGE_ENCRYPTION_DIR="${BASE_DIR}/opensearch-storage-encryption"
OPENSEARCH_DIST_DIR="${OPENSEARCH_DIR}/build/distribution/local/opensearch-${OPENSEARCH_VERSION}"
JVM_HEAP_SIZE="4g"
JVM_DIRECT_MEM_SIZE="4g"
DEBUG_PORT="5005"

# Create and navigate to your workspace directory
mkdir -p "${BASE_DIR}" && cd "${BASE_DIR}"

# Clone OpenSearch
git clone https://github.com/opensearch-project/OpenSearch.git "${OPENSEARCH_DIR}"

# Clone Storage Encryption plugin
git clone https://github.com/opensearch-project/opensearch-storage-encryption.git "${STORAGE_ENCRYPTION_DIR}"

# Build Storage Encryption plugin
cd "${STORAGE_ENCRYPTION_DIR}"
./gradlew clean assemble

# Build Crypto KMS plugin (required dependency)
cd "${OPENSEARCH_DIR}"
./gradlew :plugins:crypto-kms:assemble

# Build local distribution
./gradlew localDistro
```

## Installing and Configuring Plugins

```bash
# Navigate to the OpenSearch distribution directory
cd "${OPENSEARCH_DIST_DIR}/bin"

# Install Storage Encryption plugin
./opensearch-plugin install file:${STORAGE_ENCRYPTION_DIR}/build/distributions/storage-encryption.zip

# Install Crypto KMS plugin
./opensearch-plugin install file:${OPENSEARCH_DIR}/plugins/crypto-kms/build/distributions/crypto-kms-${OPENSEARCH_VERSION}.zip

# Create keystore and add credentials from environment variables
./opensearch-keystore create
echo "${AWS_SESSION_TOKEN}" | ./opensearch-keystore add -x kms.session_token
echo "${AWS_ACCESS_KEY_ID}" | ./opensearch-keystore add -x kms.access_key
echo "${AWS_SECRET_ACCESS_KEY}" | ./opensearch-keystore add -x kms.secret_key

# Append KMS configuration to opensearch.yml
cat >> "${OPENSEARCH_DIST_DIR}/config/opensearch.yml" << EOF
# KMS Configuration
kms.region: ${KMS_REGION}
kms.key_arn: ${KMS_KEY_ARN}
EOF

# Update JVM settings
JVM_OPTIONS_FILE="${OPENSEARCH_DIST_DIR}/config/jvm.options"

# Update heap size (
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Running macOS version..."
    sed -i '' "s/-Xms1g/-Xms${JVM_HEAP_SIZE}/g" "${JVM_OPTIONS_FILE}"
    sed -i '' "s/-Xmx1g/-Xmx${JVM_HEAP_SIZE}/g" "${JVM_OPTIONS_FILE}"
else
    echo "Running Linux version..."
    sed -i "s/-Xms1g/-Xms${JVM_HEAP_SIZE}/g" "${JVM_OPTIONS_FILE}"
    sed -i "s/-Xmx1g/-Xmx${JVM_HEAP_SIZE}/g" "${JVM_OPTIONS_FILE}"
fi


add_jvm_option() {
    local option="$1"
    if ! grep -q "^${option}$" "${JVM_OPTIONS_FILE}"; then
        echo "${option}" >> "${JVM_OPTIONS_FILE}"
    fi
}

# Add required JVM options
add_jvm_option "-XX:MaxDirectMemorySize=${JVM_DIRECT_MEM_SIZE}"
```

## Running and Testing OpenSearch

```bash
# Start OpenSearch
./opensearch
```

## Running Tests

### Unit Tests

```bash
cd "${STORAGE_ENCRYPTION_DIR}"
./gradlew test
```

### Integration Tests

```bash
./gradlew integrationTest
```

### YAML Rest Tests

```bash
./gradlew yamlRestTest
```

## Debugging

To debug the plugin:

```bash
# Verify environment variables
echo "JVM_OPTIONS_FILE=${JVM_OPTIONS_FILE}"
echo "DEBUG_PORT=${DEBUG_PORT}"

# Add debug options to JVM configuration
if ! grep -F '-Xdebug' "${JVM_OPTIONS_FILE}" > /dev/null; then
    echo '-Xdebug' >> "${JVM_OPTIONS_FILE}"
fi

# Add debug port configuration
DEBUG_STRING="-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=*:${DEBUG_PORT}"
if ! grep -F "${DEBUG_STRING}" "${JVM_OPTIONS_FILE}" > /dev/null; then
    echo "${DEBUG_STRING}" >> "${JVM_OPTIONS_FILE}"
fi

# Verify the changes
echo "Updated JVM debug options:"
grep -F 'Xdebug\|Xrunjdwp' "${JVM_OPTIONS_FILE}"

# Connect your IDE debugger to the specified debug port
```

## Architecture Deep Dive

### Plugin Registration

The `CryptoDirectoryPlugin` registers `"cryptofs"` as a custom store type:

```java
@Override
public Map<String, DirectoryFactory> getDirectoryFactories() {
    return Collections.singletonMap("cryptofs", new CryptoDirectoryFactory());
}
```

When an index is created with `index.store.type: "cryptofs"`, the `CryptoDirectoryFactory` is invoked to create the encrypted directory.

### Directory Selection Logic

The `CryptoDirectoryFactory.newFSDirectory()` method selects the appropriate encrypted directory implementation based on the node's default store type:

- **HYBRIDFS** → `HybridCryptoDirectory` (wraps `CryptoDirectIODirectory` + handles NIO extensions)
- **MMAPFS** → `CryptoDirectIODirectory` (MMAP not natively supported)
- **NIOFS/SIMPLEFS** → `CryptoNIOFSDirectory`

### Encryption Flow

**Write Path:**
1. Lucene writes data through the Directory interface
2. `CryptoDirectory` intercepts the write operation
3. Data is encrypted block-by-block using the shard's data key
4. Encrypted blocks are written to disk
5. Encryption metadata (IV, key version) stored in file footer

**Read Path:**
1. Lucene requests data through the Directory interface
2. Check block cache for decrypted data
3. If cache miss:
   - Load encrypted block from disk
   - Read encryption metadata from file footer
   - Decrypt block using shard's data key
   - Store decrypted block in cache
4. Return decrypted data to Lucene

### Key Management Hierarchy

```
Master Key Provider
    ↓ (encrypt/decrypt data key)
NodeLevelKeyCache (encrypted data keys)
    ↓ (decrypt & HKDF derivation)
ShardKeyResolverRegistry (per-shard data keys)
    ↓ (used by)
CryptoDirectory implementations
```

### Memory Architecture

**Shared Resources (Node-Level):**
- `MemorySegmentPool`: Off-heap memory pool for encryption operations
- `BlockCache`: Caffeine cache for decrypted blocks
- `ReadAheadExecutor`: Thread pool for async prefetching

**Per-Directory Resources:**
- `BlockLoader`: Directory-specific decryption logic
- `ReadAheadWorker`: Per-shard queue with shared executor threads
- `KeyResolver`: Shard-specific key resolution


## Testing

### Test Categories

The plugin includes comprehensive test coverage across multiple test types:

#### 1. Unit Tests (`./gradlew test`)

Located in `src/test/java/`, these test individual components:

- **Cipher Tests** (`cipher/`): Encryption/decryption operations
- **Block Cache Tests** (`block_cache/`): Cache behavior and eviction
- **Key Management Tests** (`key/`): Key derivation and caching
- **Memory Pool Tests** (`pool/`): Memory allocation and pooling
- **Read-Ahead Tests** (`read_ahead/`): Prefetching logic

#### 2. Integration Tests (`./gradlew internalClusterTest`)

Located in `src/internalClusterTest/java/`, these test cluster-level behavior:

- **CryptoDirectoryIntegTestCases**: Basic CRUD operations with encryption
- **ShardMigrationIntegTests**: Shard relocation and recovery
- **SnapshotRestoreIntegTests**: Snapshot/restore with encrypted indices
- **ConcurrencyIntegTests**: Multi-threaded access patterns
- **IndexTemplateIntegTests**: Template-based index creation
- **CacheInvalidationIntegTests**: Cache invalidation on index deletion

#### 3. YAML REST Tests (`./gradlew yamlRestTest`)

Located in `src/yamlRestTest/resources/rest-api-spec/test/`:

- `10_basic.yml`: Basic plugin functionality
- `20_encrypted_index_crud.yml`: Index CRUD operations
- `30_bulk_operations.yml`: Bulk indexing and searching
- `40_snapshot_restore.yml`: Snapshot/restore workflows
- `50_translog_operations.yml`: Translog encryption
- `60_cache_stress_tests.yml`: Cache stress testing

### Running Specific Tests

```bash
# Run a specific test class
./gradlew test --tests "CipherEncryptionDecryptionTests"

# Run tests matching a pattern
./gradlew test --tests "*Cache*"

# Run integration tests for a specific class
./gradlew internalClusterTest --tests "ShardMigrationIntegTests"

# Run a specific YAML test
./gradlew yamlRestTest --tests "*20_encrypted_index_crud*"
```

### Test Coverage

Generate test coverage report:

```bash
./gradlew test jacocoTestReport

# Report location: build/reports/jacoco/test/html/index.html
```

### Writing Tests

When contributing tests:

1. **Unit Tests**: Mock external dependencies (KMS, file system)
2. **Integration Tests**: Use `MockCryptoPlugin` for deterministic behavior
3. **YAML Tests**: Focus on REST API behavior and user workflows

Example test setup:

```java
@OpenSearchIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class MyIntegrationTest extends OpenSearchIntegTestCase {
    
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            CryptoDirectoryPlugin.class,
            MockCryptoKeyProviderPlugin.class
        );
    }
    
    @Test
    public void testEncryptedIndexCreation() {
        // Test implementation
    }
}
```

## Code Style

This project uses the OpenSearch code style with Spotless for formatting.

### Apply Formatting

```bash
# Check formatting
./gradlew spotlessCheck

# Apply formatting
./gradlew spotlessApply
```

### Code Style Rules

- **License Headers**: All files must have Apache 2.0 license header
- **Import Order**: java, javax, org, com
- **Eclipse Formatter**: Uses `.eclipseformat.xml` configuration
- **Line Length**: Maximum 120 characters (where practical)

## Contributing

### Contribution Workflow

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/my-feature`)
3. **Make** your changes
4. **Run** tests and formatting:
   ```bash
   ./gradlew spotlessApply
   ./gradlew check
   ```
5. **Commit** with clear messages
6. **Push** to your fork
7. **Submit** a pull request

### PR Guidelines

- Include test coverage for new features
- Update documentation (README, JavaDoc)
- Follow existing code patterns
- Keep PRs focused and reasonably sized
- Reference any related issues

### Code Review Process

- At least one approval required
- All tests must pass
- Code style checks must pass
- No merge conflicts

## Performance Considerations

### Profiling

For performance analysis:

```bash
# Enable JFR recording
-XX:StartFlightRecording=filename=recording.jfr,duration=60s

# Analyze with profiler.amazon.com
# Upload recording.jfr to internal profiling tools
```

### Key Metrics

Monitor these metrics in production:

- **Encryption Throughput**: Operations per second
- **Cache Hit Rate**: Target > 90% for read-heavy workloads
- **Memory Pool Utilization**: Monitor via telemetry logs
- **Key Refresh Latency**: Should be < 100ms

## Troubleshooting

### Common Issues

#### Plugin Installation Fails
- **Cause**: Version mismatch between OpenSearch and plugin
- **Solution**: Ensure compatible versions (`3.3.0-SNAPSHOT`)

#### KMS Integration Issues
- **Cause**: Invalid AWS credentials or permissions
- **Solution**: 
  - Verify credentials in keystore
  - Check KMS key policy allows encrypt/decrypt operations
  - Test with AWS CLI: `aws kms encrypt --key-id <arn> --plaintext "test"`

#### Memory Issues
- **Symptoms**: `OutOfMemoryError` or slow performance
- **Solutions**:
  - Increase heap: `-Xmx4g`
  - Increase direct memory: `-XX:MaxDirectMemorySize=4g`
  - Reduce pool size: `node.store.crypto.pool_size_percentage: 0.2`
  - Reduce cache ratio: `node.store.crypto.cache_to_pool_ratio: 0.5`

#### Build Failures
- **Cause**: Missing dependencies or JDK version mismatch
- **Solutions**:
  - Clean build: `./gradlew clean`
  - Verify JDK: `java -version` (must be 21+)
  - Check Gradle: `./gradlew --version`

#### Test Failures
- **Integration Tests**: Ensure crypto-kms plugin is built
- **AWS Tests**: Check AWS credentials and permissions
- **Timing Issues**: Increase timeouts in CI environments

### Debug Logging

Enable detailed logging in `log4j2.properties`:

```properties
logger.crypto.name = org.opensearch.index.store
logger.crypto.level = debug

logger.key.name = org.opensearch.index.store.key
logger.key.level = trace
```

### Getting Help

- **Issues**: [GitHub Issues](https://github.com/opensearch-project/opensearch-storage-encryption/issues)
- **Discussions**: [OpenSearch Forums](https://forum.opensearch.org/)
- **Slack**: OpenSearch community Slack workspace

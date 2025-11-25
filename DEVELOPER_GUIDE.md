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


## Testing

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


## Performance Considerations

OpenSearch Benchmark results on http_logs workload on c5.12xLarge instance:

**Workload**:

```
opensearch-benchmark execute-test \
  --kill-running-processes \
  --pipeline=benchmark-only \
  --workload=http_logs \
  --target-hosts=http://localhost:9200 \
  --exclude-tasks="\
term,\
range,\
default,\
status-200s-in-range,\
status-400s-in-range,\
hourly_agg,\
hourly_agg_with_filter,\
hourly_agg_with_filter_and_metrics,\
multi_term_agg,\
scroll,\
desc_sort_size,\
asc_sort_size,\
desc_sort_timestamp,\
asc_sort_timestamp,\
desc_sort_with_after_timestamp,\
asc_sort_with_after_timestamp,\
force-merge,\
refresh-after-force-merge,\
wait-until-merges-finish,\
force-merge-1-seg,\
refresh-after-force-merge-1-seg,\
wait-until-merges-1-seg-finish,\
desc-sort-timestamp-after-force-merge-1-seg,\
asc-sort-timestamp-after-force-merge-1-seg,\
desc-sort-with-after-timestamp-after-force-merge-1-seg,\
asc-sort-with-after-timestamp-after-force-merge-1-seg" \
--workload-params '{
     "number_of_shards": "1",
     "number_of_replicas": "0",
     "index_settings": {
        "index.store.type": "cryptofs",
        "index.store.crypto.kms.key_arn":"arn:aws:kms:us-east-1:1234567890:key/abcdefgh-1234-123a-a123b-abcdefghijklm",
        "index.store.crypto.key_provider": "aws-kms"
        }
    }'
```
**Results:**

```
|                                                         Metric |         Task |     Value |   Unit |
|---------------------------------------------------------------:|-------------:|----------:|-------:|
|                     Cumulative indexing time of primary shards |              |   89.5535 |    min |
|             Min cumulative indexing time across primary shards |              |         0 |    min |
|          Median cumulative indexing time across primary shards |              |  0.786783 |    min |
|             Max cumulative indexing time across primary shards |              |   13.4496 |    min |
|            Cumulative indexing throttle time of primary shards |              |         0 |    min |
|    Min cumulative indexing throttle time across primary shards |              |         0 |    min |
| Median cumulative indexing throttle time across primary shards |              |         0 |    min |
|    Max cumulative indexing throttle time across primary shards |              |         0 |    min |
|                        Cumulative merge time of primary shards |              |   40.4497 |    min |
|                       Cumulative merge count of primary shards |              |       216 |        |
|                Min cumulative merge time across primary shards |              |         0 |    min |
|             Median cumulative merge time across primary shards |              |  0.101675 |    min |
|                Max cumulative merge time across primary shards |              |   8.78198 |    min |
|               Cumulative merge throttle time of primary shards |              |   27.8899 |    min |
|       Min cumulative merge throttle time across primary shards |              |         0 |    min |
|    Median cumulative merge throttle time across primary shards |              | 0.0510583 |    min |
|       Max cumulative merge throttle time across primary shards |              |   6.46397 |    min |
|                      Cumulative refresh time of primary shards |              |   3.69217 |    min |
|                     Cumulative refresh count of primary shards |              |       419 |        |
|              Min cumulative refresh time across primary shards |              |         0 |    min |
|           Median cumulative refresh time across primary shards |              |  0.050225 |    min |
|              Max cumulative refresh time across primary shards |              |  0.597767 |    min |
|                        Cumulative flush time of primary shards |              |   9.14458 |    min |
|                       Cumulative flush count of primary shards |              |       110 |        |
|                Min cumulative flush time across primary shards |              |         0 |    min |
|             Median cumulative flush time across primary shards |              |    0.0075 |    min |
|                Max cumulative flush time across primary shards |              |   1.68112 |    min |
|                                        Total Young Gen GC time |              |    10.557 |      s |
|                                       Total Young Gen GC count |              |       780 |        |
|                                          Total Old Gen GC time |              |         0 |      s |
|                                         Total Old Gen GC count |              |         0 |        |
|                                                     Store size |              |   20.3044 |     GB |
|                                                  Translog size |              |  0.855037 |     GB |
|                                         Heap used for segments |              |         0 |     MB |
|                                       Heap used for doc values |              |         0 |     MB |
|                                            Heap used for terms |              |         0 |     MB |
|                                            Heap used for norms |              |         0 |     MB |
|                                           Heap used for points |              |         0 |     MB |
|                                    Heap used for stored fields |              |         0 |     MB |
|                                                  Segment count |              |       358 |        |
|                                                 Min Throughput | index-append |    337229 | docs/s |
|                                                Mean Throughput | index-append |    373391 | docs/s |
|                                              Median Throughput | index-append |    362997 | docs/s |
|                                                 Max Throughput | index-append |    445108 | docs/s |
|                                        50th percentile latency | index-append |   62.4251 |     ms |
|                                        90th percentile latency | index-append |   295.983 |     ms |
|                                        99th percentile latency | index-append |   608.552 |     ms |
|                                      99.9th percentile latency | index-append |   979.953 |     ms |
|                                     99.99th percentile latency | index-append |   1150.74 |     ms |
|                                       100th percentile latency | index-append |   1410.53 |     ms |
|                                   50th percentile service time | index-append |   62.4251 |     ms |
|                                   90th percentile service time | index-append |   295.983 |     ms |
|                                   99th percentile service time | index-append |   608.552 |     ms |
|                                 99.9th percentile service time | index-append |   979.953 |     ms |
|                                99.99th percentile service time | index-append |   1150.74 |     ms |
|                                  100th percentile service time | index-append |   1410.53 |     ms |
|                                                     error rate | index-append |         0 |      % |


---------------------------------
[INFO] SUCCESS (took 769 seconds)
---------------------------------
```

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

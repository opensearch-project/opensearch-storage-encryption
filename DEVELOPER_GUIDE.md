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
  --workload=big5 \
  --test-mode \
  --target-hosts=http://localhost:9200 \
  --workload-params '{
    "number_of_shards": "5",
    "number_of_replicas": "0",
    "index_settings": {
    "index.store.type": "cryptofs",
    "index.store.crypto.key_provider":"aws-kms",
    "index.store.crypto.kms.key_arn":"<kms_key>"
    }
  }'
```
**Results:**
```

|                                                         Metric |                                                 Task |       Value |    Unit |
|---------------------------------------------------------------:|-----------------------------------------------------:|------------:|--------:|
|                                                     Store size |                                                      | 0.000851813 |      GB |
|                                                  Translog size |                                                      | 0.000923934 |      GB |
|                                         Heap used for segments |                                                      |           0 |      MB |
|                                       Heap used for doc values |                                                      |           0 |      MB |
|                                            Heap used for terms |                                                      |           0 |      MB |
|                                            Heap used for norms |                                                      |           0 |      MB |
|                                           Heap used for points |                                                      |           0 |      MB |
|                                    Heap used for stored fields |                                                      |           0 |      MB |
|                                                  Segment count |                                                      |          23 |         |
|                                                 Min Throughput |                                         index-append |     3420.01 |  docs/s |
|                                                Mean Throughput |                                         index-append |     3420.01 |  docs/s |
|                                              Median Throughput |                                         index-append |     3420.01 |  docs/s |
|                                                 Max Throughput |                                         index-append |     3420.01 |  docs/s |
|                                        50th percentile latency |                                         index-append |     280.861 |      ms |
|                                       100th percentile latency |                                         index-append |     288.973 |      ms |
|                                   50th percentile service time |                                         index-append |     280.861 |      ms |
|                                  100th percentile service time |                                         index-append |     288.973 |      ms |
|                                                     error rate |                                         index-append |           0 |       % |
|                                                 Min Throughput |                             wait-until-merges-finish |        77.9 |   ops/s |
|                                                Mean Throughput |                             wait-until-merges-finish |        77.9 |   ops/s |
|                                              Median Throughput |                             wait-until-merges-finish |        77.9 |   ops/s |
|                                                 Max Throughput |                             wait-until-merges-finish |        77.9 |   ops/s |
|                                       100th percentile latency |                             wait-until-merges-finish |     12.5842 |      ms |
|                                  100th percentile service time |                             wait-until-merges-finish |     12.5842 |      ms |
|                                                     error rate |                             wait-until-merges-finish |           0 |       % |
|                                                 Min Throughput |                                              default |       11.59 |   ops/s |
|                                                Mean Throughput |                                              default |       11.59 |   ops/s |
|                                              Median Throughput |                                              default |       11.59 |   ops/s |
|                                                 Max Throughput |                                              default |       11.59 |   ops/s |
|                                       100th percentile latency |                                              default |     93.0691 |      ms |
|                                  100th percentile service time |                                              default |     6.50284 |      ms |
|                                                     error rate |                                              default |           0 |       % |
|                                                 Min Throughput |                                  desc_sort_timestamp |       24.11 |   ops/s |
|                                                Mean Throughput |                                  desc_sort_timestamp |       24.11 |   ops/s |
|                                              Median Throughput |                                  desc_sort_timestamp |       24.11 |   ops/s |
|                                                 Max Throughput |                                  desc_sort_timestamp |       24.11 |   ops/s |
|                                       100th percentile latency |                                  desc_sort_timestamp |     49.8237 |      ms |
|                                  100th percentile service time |                                  desc_sort_timestamp |     8.12369 |      ms |
|                                                     error rate |                                  desc_sort_timestamp |           0 |       % |
|                                                 Min Throughput |                                   asc_sort_timestamp |       78.21 |   ops/s |
|                                                Mean Throughput |                                   asc_sort_timestamp |       78.21 |   ops/s |
|                                              Median Throughput |                                   asc_sort_timestamp |       78.21 |   ops/s |
|                                                 Max Throughput |                                   asc_sort_timestamp |       78.21 |   ops/s |
|                                       100th percentile latency |                                   asc_sort_timestamp |       21.96 |      ms |
|                                  100th percentile service time |                                   asc_sort_timestamp |     8.97667 |      ms |
|                                                     error rate |                                   asc_sort_timestamp |           0 |       % |
|                                                 Min Throughput |                       desc_sort_with_after_timestamp |       59.36 |   ops/s |
|                                                Mean Throughput |                       desc_sort_with_after_timestamp |       59.36 |   ops/s |
|                                              Median Throughput |                       desc_sort_with_after_timestamp |       59.36 |   ops/s |
|                                                 Max Throughput |                       desc_sort_with_after_timestamp |       59.36 |   ops/s |
|                                       100th percentile latency |                       desc_sort_with_after_timestamp |     24.0916 |      ms |
|                                  100th percentile service time |                       desc_sort_with_after_timestamp |     7.03658 |      ms |
|                                                     error rate |                       desc_sort_with_after_timestamp |           0 |       % |
|                                                 Min Throughput |                        asc_sort_with_after_timestamp |      111.57 |   ops/s |
|                                                Mean Throughput |                        asc_sort_with_after_timestamp |      111.57 |   ops/s |
|                                              Median Throughput |                        asc_sort_with_after_timestamp |      111.57 |   ops/s |
|                                                 Max Throughput |                        asc_sort_with_after_timestamp |      111.57 |   ops/s |
|                                       100th percentile latency |                        asc_sort_with_after_timestamp |     13.5115 |      ms |
|                                  100th percentile service time |                        asc_sort_with_after_timestamp |     4.36592 |      ms |
|                                                     error rate |                        asc_sort_with_after_timestamp |           0 |       % |
|                                                 Min Throughput |               desc_sort_timestamp_can_match_shortcut |       37.42 |   ops/s |
|                                                Mean Throughput |               desc_sort_timestamp_can_match_shortcut |       37.42 |   ops/s |
|                                              Median Throughput |               desc_sort_timestamp_can_match_shortcut |       37.42 |   ops/s |
|                                                 Max Throughput |               desc_sort_timestamp_can_match_shortcut |       37.42 |   ops/s |
|                                       100th percentile latency |               desc_sort_timestamp_can_match_shortcut |     33.0122 |      ms |
|                                  100th percentile service time |               desc_sort_timestamp_can_match_shortcut |     6.08784 |      ms |
|                                                     error rate |               desc_sort_timestamp_can_match_shortcut |           0 |       % |
|                                                 Min Throughput |            desc_sort_timestamp_no_can_match_shortcut |      108.23 |   ops/s |
|                                                Mean Throughput |            desc_sort_timestamp_no_can_match_shortcut |      108.23 |   ops/s |
|                                              Median Throughput |            desc_sort_timestamp_no_can_match_shortcut |      108.23 |   ops/s |
|                                                 Max Throughput |            desc_sort_timestamp_no_can_match_shortcut |      108.23 |   ops/s |
|                                       100th percentile latency |            desc_sort_timestamp_no_can_match_shortcut |     14.3219 |      ms |
|                                  100th percentile service time |            desc_sort_timestamp_no_can_match_shortcut |     4.87829 |      ms |
|                                                     error rate |            desc_sort_timestamp_no_can_match_shortcut |           0 |       % |
|                                                 Min Throughput |                asc_sort_timestamp_can_match_shortcut |      100.26 |   ops/s |
|                                                Mean Throughput |                asc_sort_timestamp_can_match_shortcut |      100.26 |   ops/s |
|                                              Median Throughput |                asc_sort_timestamp_can_match_shortcut |      100.26 |   ops/s |
|                                                 Max Throughput |                asc_sort_timestamp_can_match_shortcut |      100.26 |   ops/s |
|                                       100th percentile latency |                asc_sort_timestamp_can_match_shortcut |     15.8897 |      ms |
|                                  100th percentile service time |                asc_sort_timestamp_can_match_shortcut |     5.74301 |      ms |
|                                                     error rate |                asc_sort_timestamp_can_match_shortcut |           0 |       % |
|                                                 Min Throughput |             asc_sort_timestamp_no_can_match_shortcut |      113.14 |   ops/s |
|                                                Mean Throughput |             asc_sort_timestamp_no_can_match_shortcut |      113.14 |   ops/s |
|                                              Median Throughput |             asc_sort_timestamp_no_can_match_shortcut |      113.14 |   ops/s |
|                                                 Max Throughput |             asc_sort_timestamp_no_can_match_shortcut |      113.14 |   ops/s |
|                                       100th percentile latency |             asc_sort_timestamp_no_can_match_shortcut |     13.6954 |      ms |
|                                  100th percentile service time |             asc_sort_timestamp_no_can_match_shortcut |     4.67173 |      ms |
|                                                     error rate |             asc_sort_timestamp_no_can_match_shortcut |           0 |       % |
|                                                 Min Throughput |                                                 term |      116.34 |   ops/s |
|                                                Mean Throughput |                                                 term |      116.34 |   ops/s |
|                                              Median Throughput |                                                 term |      116.34 |   ops/s |
|                                                 Max Throughput |                                                 term |      116.34 |   ops/s |
|                                       100th percentile latency |                                                 term |     16.6352 |      ms |
|                                  100th percentile service time |                                                 term |     7.87475 |      ms |
|                                                     error rate |                                                 term |           0 |       % |
|                                                 Min Throughput |                                  multi_terms-keyword |       19.48 |   ops/s |
|                                                Mean Throughput |                                  multi_terms-keyword |       19.48 |   ops/s |
|                                              Median Throughput |                                  multi_terms-keyword |       19.48 |   ops/s |
|                                                 Max Throughput |                                  multi_terms-keyword |       19.48 |   ops/s |
|                                       100th percentile latency |                                  multi_terms-keyword |      57.183 |      ms |
|                                  100th percentile service time |                                  multi_terms-keyword |     5.62188 |      ms |
|                                                     error rate |                                  multi_terms-keyword |           0 |       % |
|                                                 Min Throughput |                                        keyword-terms |       24.09 |   ops/s |
|                                                Mean Throughput |                                        keyword-terms |       24.09 |   ops/s |
|                                              Median Throughput |                                        keyword-terms |       24.09 |   ops/s |
|                                                 Max Throughput |                                        keyword-terms |       24.09 |   ops/s |
|                                       100th percentile latency |                                        keyword-terms |     53.1469 |      ms |
|                                  100th percentile service time |                                        keyword-terms |      11.412 |      ms |
|                                                     error rate |                                        keyword-terms |           0 |       % |
|                                                 Min Throughput |                        keyword-terms-low-cardinality |       80.23 |   ops/s |
|                                                Mean Throughput |                        keyword-terms-low-cardinality |       80.23 |   ops/s |
|                                              Median Throughput |                        keyword-terms-low-cardinality |       80.23 |   ops/s |
|                                                 Max Throughput |                        keyword-terms-low-cardinality |       80.23 |   ops/s |
|                                       100th percentile latency |                        keyword-terms-low-cardinality |     19.9721 |      ms |
|                                  100th percentile service time |                        keyword-terms-low-cardinality |     7.29634 |      ms |
|                                                     error rate |                        keyword-terms-low-cardinality |           0 |       % |
|                                                 Min Throughput |                                      composite-terms |        50.3 |   ops/s |
|                                                Mean Throughput |                                      composite-terms |        50.3 |   ops/s |
|                                              Median Throughput |                                      composite-terms |        50.3 |   ops/s |
|                                                 Max Throughput |                                      composite-terms |        50.3 |   ops/s |
|                                       100th percentile latency |                                      composite-terms |      23.999 |      ms |
|                                  100th percentile service time |                                      composite-terms |      3.9001 |      ms |
|                                                     error rate |                                      composite-terms |           0 |       % |
|                                                 Min Throughput |                              composite_terms-keyword |      120.49 |   ops/s |
|                                                Mean Throughput |                              composite_terms-keyword |      120.49 |   ops/s |
|                                              Median Throughput |                              composite_terms-keyword |      120.49 |   ops/s |
|                                                 Max Throughput |                              composite_terms-keyword |      120.49 |   ops/s |
|                                       100th percentile latency |                              composite_terms-keyword |     12.1194 |      ms |
|                                  100th percentile service time |                              composite_terms-keyword |     3.63297 |      ms |
|                                                     error rate |                              composite_terms-keyword |           0 |       % |
|                                                 Min Throughput |                       composite-date_histogram-daily |       34.28 |   ops/s |
|                                                Mean Throughput |                       composite-date_histogram-daily |       34.28 |   ops/s |
|                                              Median Throughput |                       composite-date_histogram-daily |       34.28 |   ops/s |
|                                                 Max Throughput |                       composite-date_histogram-daily |       34.28 |   ops/s |
|                                       100th percentile latency |                       composite-date_histogram-daily |     33.7504 |      ms |
|                                  100th percentile service time |                       composite-date_histogram-daily |     4.31118 |      ms |
|                                                     error rate |                       composite-date_histogram-daily |           0 |       % |
|                                                 Min Throughput |                                                range |      103.37 |   ops/s |
|                                                Mean Throughput |                                                range |      103.37 |   ops/s |
|                                              Median Throughput |                                                range |      103.37 |   ops/s |
|                                                 Max Throughput |                                                range |      103.37 |   ops/s |
|                                       100th percentile latency |                                                range |     14.0325 |      ms |
|                                  100th percentile service time |                                                range |     4.15609 |      ms |
|                                                     error rate |                                                range |           0 |       % |
|                                                 Min Throughput |                                        range-numeric |      119.92 |   ops/s |
|                                                Mean Throughput |                                        range-numeric |      119.92 |   ops/s |
|                                              Median Throughput |                                        range-numeric |      119.92 |   ops/s |
|                                                 Max Throughput |                                        range-numeric |      119.92 |   ops/s |
|                                       100th percentile latency |                                        range-numeric |      10.905 |      ms |
|                                  100th percentile service time |                                        range-numeric |     2.39537 |      ms |
|                                                     error rate |                                        range-numeric |           0 |       % |
|                                                 Min Throughput |                                     keyword-in-range |       78.74 |   ops/s |
|                                                Mean Throughput |                                     keyword-in-range |       78.74 |   ops/s |
|                                              Median Throughput |                                     keyword-in-range |       78.74 |   ops/s |
|                                                 Max Throughput |                                     keyword-in-range |       78.74 |   ops/s |
|                                       100th percentile latency |                                     keyword-in-range |     17.7274 |      ms |
|                                  100th percentile service time |                                     keyword-in-range |     4.85168 |      ms |
|                                                     error rate |                                     keyword-in-range |           0 |       % |
|                                                 Min Throughput |                            date_histogram_hourly_agg |       78.78 |   ops/s |
|                                                Mean Throughput |                            date_histogram_hourly_agg |       78.78 |   ops/s |
|                                              Median Throughput |                            date_histogram_hourly_agg |       78.78 |   ops/s |
|                                                 Max Throughput |                            date_histogram_hourly_agg |       78.78 |   ops/s |
|                                       100th percentile latency |                            date_histogram_hourly_agg |     16.4859 |      ms |
|                                  100th percentile service time |                            date_histogram_hourly_agg |     3.61588 |      ms |
|                                                     error rate |                            date_histogram_hourly_agg |           0 |       % |
|                                                 Min Throughput |                            date_histogram_minute_agg |       66.56 |   ops/s |
|                                                Mean Throughput |                            date_histogram_minute_agg |       66.56 |   ops/s |
|                                              Median Throughput |                            date_histogram_minute_agg |       66.56 |   ops/s |
|                                                 Max Throughput |                            date_histogram_minute_agg |       66.56 |   ops/s |
|                                       100th percentile latency |                            date_histogram_minute_agg |     22.7048 |      ms |
|                                  100th percentile service time |                            date_histogram_minute_agg |     7.46428 |      ms |
|                                                     error rate |                            date_histogram_minute_agg |           0 |       % |
|                                                 Min Throughput |                                               scroll |       39.13 | pages/s |
|                                                Mean Throughput |                                               scroll |       39.13 | pages/s |
|                                              Median Throughput |                                               scroll |       39.13 | pages/s |
|                                                 Max Throughput |                                               scroll |       39.13 | pages/s |
|                                       100th percentile latency |                                               scroll |     78.7426 |      ms |
|                                  100th percentile service time |                                               scroll |      25.365 |      ms |
|                                                     error rate |                                               scroll |           0 |       % |
|                                                 Min Throughput |                              query-string-on-message |       26.57 |   ops/s |
|                                                Mean Throughput |                              query-string-on-message |       26.57 |   ops/s |
|                                              Median Throughput |                              query-string-on-message |       26.57 |   ops/s |
|                                                 Max Throughput |                              query-string-on-message |       26.57 |   ops/s |
|                                       100th percentile latency |                              query-string-on-message |     43.2774 |      ms |
|                                  100th percentile service time |                              query-string-on-message |     5.44084 |      ms |
|                                                     error rate |                              query-string-on-message |           0 |       % |
|                                                 Min Throughput |                     query-string-on-message-filtered |      144.89 |   ops/s |
|                                                Mean Throughput |                     query-string-on-message-filtered |      144.89 |   ops/s |
|                                              Median Throughput |                     query-string-on-message-filtered |      144.89 |   ops/s |
|                                                 Max Throughput |                     query-string-on-message-filtered |      144.89 |   ops/s |
|                                       100th percentile latency |                     query-string-on-message-filtered |     9.69815 |      ms |
|                                  100th percentile service time |                     query-string-on-message-filtered |        2.61 |      ms |
|                                                     error rate |                     query-string-on-message-filtered |           0 |       % |
|                                                 Min Throughput |          query-string-on-message-filtered-sorted-num |      101.87 |   ops/s |
|                                                Mean Throughput |          query-string-on-message-filtered-sorted-num |      101.87 |   ops/s |
|                                              Median Throughput |          query-string-on-message-filtered-sorted-num |      101.87 |   ops/s |
|                                                 Max Throughput |          query-string-on-message-filtered-sorted-num |      101.87 |   ops/s |
|                                       100th percentile latency |          query-string-on-message-filtered-sorted-num |     13.9978 |      ms |
|                                  100th percentile service time |          query-string-on-message-filtered-sorted-num |     3.97487 |      ms |
|                                                     error rate |          query-string-on-message-filtered-sorted-num |           0 |       % |
|                                                 Min Throughput |                      sort_keyword_can_match_shortcut |       54.17 |   ops/s |
|                                                Mean Throughput |                      sort_keyword_can_match_shortcut |       54.17 |   ops/s |
|                                              Median Throughput |                      sort_keyword_can_match_shortcut |       54.17 |   ops/s |
|                                                 Max Throughput |                      sort_keyword_can_match_shortcut |       54.17 |   ops/s |
|                                       100th percentile latency |                      sort_keyword_can_match_shortcut |     23.8364 |      ms |
|                                  100th percentile service time |                      sort_keyword_can_match_shortcut |     5.17946 |      ms |
|                                                     error rate |                      sort_keyword_can_match_shortcut |           0 |       % |
|                                                 Min Throughput |                   sort_keyword_no_can_match_shortcut |      126.32 |   ops/s |
|                                                Mean Throughput |                   sort_keyword_no_can_match_shortcut |      126.32 |   ops/s |
|                                              Median Throughput |                   sort_keyword_no_can_match_shortcut |      126.32 |   ops/s |
|                                                 Max Throughput |                   sort_keyword_no_can_match_shortcut |      126.32 |   ops/s |
|                                       100th percentile latency |                   sort_keyword_no_can_match_shortcut |     11.9707 |      ms |
|                                  100th percentile service time |                   sort_keyword_no_can_match_shortcut |     3.86415 |      ms |
|                                                     error rate |                   sort_keyword_no_can_match_shortcut |           0 |       % |
|                                                 Min Throughput |                                    sort_numeric_desc |       104.8 |   ops/s |
|                                                Mean Throughput |                                    sort_numeric_desc |       104.8 |   ops/s |
|                                              Median Throughput |                                    sort_numeric_desc |       104.8 |   ops/s |
|                                                 Max Throughput |                                    sort_numeric_desc |       104.8 |   ops/s |
|                                       100th percentile latency |                                    sort_numeric_desc |     14.2255 |      ms |
|                                  100th percentile service time |                                    sort_numeric_desc |     4.50058 |      ms |
|                                                     error rate |                                    sort_numeric_desc |           0 |       % |
|                                                 Min Throughput |                                     sort_numeric_asc |      114.16 |   ops/s |
|                                                Mean Throughput |                                     sort_numeric_asc |      114.16 |   ops/s |
|                                              Median Throughput |                                     sort_numeric_asc |      114.16 |   ops/s |
|                                                 Max Throughput |                                     sort_numeric_asc |      114.16 |   ops/s |
|                                       100th percentile latency |                                     sort_numeric_asc |     13.4616 |      ms |
|                                  100th percentile service time |                                     sort_numeric_asc |     4.53483 |      ms |
|                                                     error rate |                                     sort_numeric_asc |           0 |       % |
|                                                 Min Throughput |                         sort_numeric_desc_with_match |      136.51 |   ops/s |
|                                                Mean Throughput |                         sort_numeric_desc_with_match |      136.51 |   ops/s |
|                                              Median Throughput |                         sort_numeric_desc_with_match |      136.51 |   ops/s |
|                                                 Max Throughput |                         sort_numeric_desc_with_match |      136.51 |   ops/s |
|                                       100th percentile latency |                         sort_numeric_desc_with_match |     10.5717 |      ms |
|                                  100th percentile service time |                         sort_numeric_desc_with_match |     3.07198 |      ms |
|                                                     error rate |                         sort_numeric_desc_with_match |           0 |       % |
|                                                 Min Throughput |                          sort_numeric_asc_with_match |      138.78 |   ops/s |
|                                                Mean Throughput |                          sort_numeric_asc_with_match |      138.78 |   ops/s |
|                                              Median Throughput |                          sort_numeric_asc_with_match |      138.78 |   ops/s |
|                                                 Max Throughput |                          sort_numeric_asc_with_match |      138.78 |   ops/s |
|                                       100th percentile latency |                          sort_numeric_asc_with_match |     10.2126 |      ms |
|                                  100th percentile service time |                          sort_numeric_asc_with_match |     2.81856 |      ms |
|                                                     error rate |                          sort_numeric_asc_with_match |           0 |       % |
|                                                 Min Throughput |     range_field_conjunction_big_range_big_term_query |      128.68 |   ops/s |
|                                                Mean Throughput |     range_field_conjunction_big_range_big_term_query |      128.68 |   ops/s |
|                                              Median Throughput |     range_field_conjunction_big_range_big_term_query |      128.68 |   ops/s |
|                                                 Max Throughput |     range_field_conjunction_big_range_big_term_query |      128.68 |   ops/s |
|                                       100th percentile latency |     range_field_conjunction_big_range_big_term_query |     10.4188 |      ms |
|                                  100th percentile service time |     range_field_conjunction_big_range_big_term_query |     2.48575 |      ms |
|                                                     error rate |     range_field_conjunction_big_range_big_term_query |           0 |       % |
|                                                 Min Throughput |   range_field_disjunction_big_range_small_term_query |       172.8 |   ops/s |
|                                                Mean Throughput |   range_field_disjunction_big_range_small_term_query |       172.8 |   ops/s |
|                                              Median Throughput |   range_field_disjunction_big_range_small_term_query |       172.8 |   ops/s |
|                                                 Max Throughput |   range_field_disjunction_big_range_small_term_query |       172.8 |   ops/s |
|                                       100th percentile latency |   range_field_disjunction_big_range_small_term_query |     8.52763 |      ms |
|                                  100th percentile service time |   range_field_disjunction_big_range_small_term_query |     2.58171 |      ms |
|                                                     error rate |   range_field_disjunction_big_range_small_term_query |           0 |       % |
|                                                 Min Throughput | range_field_conjunction_small_range_small_term_query |       177.8 |   ops/s |
|                                                Mean Throughput | range_field_conjunction_small_range_small_term_query |       177.8 |   ops/s |
|                                              Median Throughput | range_field_conjunction_small_range_small_term_query |       177.8 |   ops/s |
|                                                 Max Throughput | range_field_conjunction_small_range_small_term_query |       177.8 |   ops/s |
|                                       100th percentile latency | range_field_conjunction_small_range_small_term_query |     9.29121 |      ms |
|                                  100th percentile service time | range_field_conjunction_small_range_small_term_query |     3.50426 |      ms |
|                                                     error rate | range_field_conjunction_small_range_small_term_query |           0 |       % |
|                                                 Min Throughput |   range_field_conjunction_small_range_big_term_query |      193.96 |   ops/s |
|                                                Mean Throughput |   range_field_conjunction_small_range_big_term_query |      193.96 |   ops/s |
|                                              Median Throughput |   range_field_conjunction_small_range_big_term_query |      193.96 |   ops/s |
|                                                 Max Throughput |   range_field_conjunction_small_range_big_term_query |      193.96 |   ops/s |
|                                       100th percentile latency |   range_field_conjunction_small_range_big_term_query |     7.64459 |      ms |
|                                  100th percentile service time |   range_field_conjunction_small_range_big_term_query |     2.32949 |      ms |
|                                                     error rate |   range_field_conjunction_small_range_big_term_query |           0 |       % |
|                                                 Min Throughput |                                range-auto-date-histo |       36.85 |   ops/s |
|                                                Mean Throughput |                                range-auto-date-histo |       36.85 |   ops/s |
|                                              Median Throughput |                                range-auto-date-histo |       36.85 |   ops/s |
|                                                 Max Throughput |                                range-auto-date-histo |       36.85 |   ops/s |
|                                       100th percentile latency |                                range-auto-date-histo |     37.0601 |      ms |
|                                  100th percentile service time |                                range-auto-date-histo |     9.68525 |      ms |
|                                                     error rate |                                range-auto-date-histo |           0 |       % |
|                                                 Min Throughput |                   range-auto-date-histo-with-metrics |       36.91 |   ops/s |
|                                                Mean Throughput |                   range-auto-date-histo-with-metrics |       36.91 |   ops/s |
|                                              Median Throughput |                   range-auto-date-histo-with-metrics |       36.91 |   ops/s |
|                                                 Max Throughput |                   range-auto-date-histo-with-metrics |       36.91 |   ops/s |
|                                       100th percentile latency |                   range-auto-date-histo-with-metrics |     39.3701 |      ms |
|                                  100th percentile service time |                   range-auto-date-histo-with-metrics |     12.0102 |      ms |
|                                                     error rate |                   range-auto-date-histo-with-metrics |           0 |       % |
|                                                 Min Throughput |                                          range-agg-1 |      121.73 |   ops/s |
|                                                Mean Throughput |                                          range-agg-1 |      121.73 |   ops/s |
|                                              Median Throughput |                                          range-agg-1 |      121.73 |   ops/s |
|                                                 Max Throughput |                                          range-agg-1 |      121.73 |   ops/s |
|                                       100th percentile latency |                                          range-agg-1 |     11.6808 |      ms |
|                                  100th percentile service time |                                          range-agg-1 |     3.25925 |      ms |
|                                                     error rate |                                          range-agg-1 |           0 |       % |
|                                                 Min Throughput |                                          range-agg-2 |      162.26 |   ops/s |
|                                                Mean Throughput |                                          range-agg-2 |      162.26 |   ops/s |
|                                              Median Throughput |                                          range-agg-2 |      162.26 |   ops/s |
|                                                 Max Throughput |                                          range-agg-2 |      162.26 |   ops/s |
|                                       100th percentile latency |                                          range-agg-2 |     9.45258 |      ms |
|                                  100th percentile service time |                                          range-agg-2 |     3.12185 |      ms |
|                                                     error rate |                                          range-agg-2 |           0 |       % |
|                                                 Min Throughput |                                  cardinality-agg-low |       28.32 |   ops/s |
|                                                Mean Throughput |                                  cardinality-agg-low |       28.32 |   ops/s |
|                                              Median Throughput |                                  cardinality-agg-low |       28.32 |   ops/s |
|                                                 Max Throughput |                                  cardinality-agg-low |       28.32 |   ops/s |
|                                       100th percentile latency |                                  cardinality-agg-low |     44.6827 |      ms |
|                                  100th percentile service time |                                  cardinality-agg-low |     9.18111 |      ms |
|                                                     error rate |                                  cardinality-agg-low |           0 |       % |
|                                                 Min Throughput |                                 cardinality-agg-high |      105.35 |   ops/s |
|                                                Mean Throughput |                                 cardinality-agg-high |      105.35 |   ops/s |
|                                              Median Throughput |                                 cardinality-agg-high |      105.35 |   ops/s |
|                                                 Max Throughput |                                 cardinality-agg-high |      105.35 |   ops/s |
|                                       100th percentile latency |                                 cardinality-agg-high |     16.4686 |      ms |
|                                  100th percentile service time |                                 cardinality-agg-high |     6.79761 |      ms |
|                                                     error rate |                                 cardinality-agg-high |           0 |       % |


--------------------------------
[INFO] SUCCESS (took 34 seconds)
--------------------------------
```

### Profiling

For performance analysis:

```bash
# Enable JFR recording
-XX:StartFlightRecording=filename=recording.jfr,duration=60s

# Analyze with profiler.amazon.com
# Upload recording.jfr to internal profiling tools
```

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

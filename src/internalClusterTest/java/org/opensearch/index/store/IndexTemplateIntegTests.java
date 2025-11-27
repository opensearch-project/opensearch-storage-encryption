/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class IndexTemplateIntegTests extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CryptoDirectoryPlugin.class, MockCryptoKeyProviderPlugin.class, MockCryptoPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("plugins.crypto.enabled", true)
            .put("node.store.crypto.pool_size_percentage", 0.05)
            .put("node.store.crypto.warmup_percentage", 0.0)
            .put("node.store.crypto.cache_to_pool_ratio", 0.8)
            .put("node.store.crypto.key_refresh_interval", "30s")
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private Settings cryptoSettings() {
        return Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "test-key-arn")
            .build();
    }

    /**
     * Tests legacy (v1) index template with encryption settings.
     * Legacy templates use simple pattern matching and apply to all matching indices.
     */
    public void testLegacyIndexTemplateBasic() throws Exception {
        logger.info("Testing legacy index template with encryption");

        // Create legacy index template for pattern "logs-*"
        Settings templateSettings = Settings
            .builder()
            .put(cryptoSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("logs-template")
            .setPatterns(Arrays.asList("logs-*"))
            .setSettings(templateSettings)
            .get();

        // Create index matching template pattern
        client().admin().indices().prepareCreate("logs-2024-01").get();
        ensureGreen("logs-2024-01");

        // Verify encryption settings were applied
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings("logs-2024-01").get();
        assertThat(settingsResponse.getSetting("logs-2024-01", "index.store.type"), equalTo("cryptofs"));
        assertThat(settingsResponse.getSetting("logs-2024-01", "index.store.crypto.key_provider"), equalTo("dummy"));

        // Index and search data to verify encryption works
        int numDocs = randomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("logs-2024-01").setSource("message", "log entry " + i, "timestamp", System.currentTimeMillis()).get();
        }
        refresh("logs-2024-01");

        SearchResponse response = client().prepareSearch("logs-2024-01").setSize(0).get();
        assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs));

        logger.info("Legacy template test completed - {} docs indexed and retrieved", numDocs);
    }

    /**
     * Tests legacy template with multiple wildcard patterns.
     */
    public void testLegacyTemplateMultiplePatterns() throws Exception {
        logger.info("Testing legacy template with multiple patterns");

        Settings templateSettings = Settings.builder().put(cryptoSettings()).put("index.number_of_shards", 1).build();

        // Create template matching multiple patterns
        client()
            .admin()
            .indices()
            .preparePutTemplate("multi-pattern-template")
            .setPatterns(Arrays.asList("metrics-*", "traces-*", "events-*"))
            .setSettings(templateSettings)
            .get();

        // Create indices matching different patterns
        String[] indices = { "metrics-2024-01", "traces-2024-01", "events-2024-01" };
        for (String index : indices) {
            client().admin().indices().prepareCreate(index).get();
        }
        ensureGreen(indices);

        // Verify all indices have encryption settings
        for (String index : indices) {
            GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(index).get();
            assertThat(settingsResponse.getSetting(index, "index.store.type"), equalTo("cryptofs"));

            // Index and verify data
            client().prepareIndex(index).setSource("data", "test").get();
        }
        refresh(indices);

        for (String index : indices) {
            SearchResponse response = client().prepareSearch(index).get();
            assertThat(response.getHits().getTotalHits().value(), greaterThan(0L));
        }

        logger.info("Multiple pattern template test completed");
    }

    /**
     * Tests legacy template with complex wildcard patterns using nested * wildcards.
     */
    public void testLegacyTemplateComplexWildcards() throws Exception {
        logger.info("Testing complex wildcard patterns");

        Settings templateSettings = Settings.builder().put(cryptoSettings()).build();

        // Pattern with multiple segments and wildcards
        client()
            .admin()
            .indices()
            .preparePutTemplate("multi-segment-template")
            .setPatterns(Arrays.asList("app-*-logs"))
            .setSettings(templateSettings)
            .get();

        client().admin().indices().prepareCreate("app-alpha-logs").get();
        client().admin().indices().prepareCreate("app-beta-logs").get();
        ensureGreen("app-alpha-logs", "app-beta-logs");

        // Verify encryption on both indices
        assertThat(
            client().admin().indices().prepareGetSettings("app-alpha-logs").get().getSetting("app-alpha-logs", "index.store.type"),
            equalTo("cryptofs")
        );
        assertThat(
            client().admin().indices().prepareGetSettings("app-beta-logs").get().getSetting("app-beta-logs", "index.store.type"),
            equalTo("cryptofs")
        );

        // Pattern with nested wildcards
        client()
            .admin()
            .indices()
            .preparePutTemplate("nested-wildcard-template")
            .setPatterns(Arrays.asList("data-*-*-prod"))
            .setSettings(templateSettings)
            .get();

        client().admin().indices().prepareCreate("data-us-east-prod").get();
        client().admin().indices().prepareCreate("data-eu-west-prod").get();
        ensureGreen("data-us-east-prod", "data-eu-west-prod");

        assertThat(
            client().admin().indices().prepareGetSettings("data-us-east-prod").get().getSetting("data-us-east-prod", "index.store.type"),
            equalTo("cryptofs")
        );

        // Pattern with wildcard at end
        client()
            .admin()
            .indices()
            .preparePutTemplate("suffix-wildcard-template")
            .setPatterns(Arrays.asList("service-*"))
            .setSettings(templateSettings)
            .get();

        client().admin().indices().prepareCreate("service-auth").get();
        client().admin().indices().prepareCreate("service-payment").get();
        ensureGreen("service-auth", "service-payment");

        assertThat(
            client().admin().indices().prepareGetSettings("service-auth").get().getSetting("service-auth", "index.store.type"),
            equalTo("cryptofs")
        );

        logger.info("Complex wildcard test completed");
    }

    /**
     * Tests template priority when multiple templates match the same index.
     * Higher order templates should take precedence.
     */
    public void testLegacyTemplatePriority() throws Exception {
        logger.info("Testing template priority ordering");

        // Low priority template
        Settings lowPrioritySettings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "low-priority-key")
            .put("index.number_of_shards", 1)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("low-priority-template")
            .setPatterns(Arrays.asList("test-*"))
            .setSettings(lowPrioritySettings)
            .setOrder(1)
            .get();

        // High priority template with different key
        Settings highPrioritySettings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "high-priority-key")
            .put("index.number_of_shards", 2)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("high-priority-template")
            .setPatterns(Arrays.asList("test-*"))
            .setSettings(highPrioritySettings)
            .setOrder(10)
            .get();

        // Create index - should use high priority template
        client().admin().indices().prepareCreate("test-priority-index").get();
        ensureGreen("test-priority-index");

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings("test-priority-index").get();
        assertThat(settingsResponse.getSetting("test-priority-index", "index.store.crypto.kms.key_arn"), equalTo("high-priority-key"));
        assertThat(settingsResponse.getSetting("test-priority-index", "index.number_of_shards"), equalTo("2"));

        // Verify encryption works with high priority settings
        client().prepareIndex("test-priority-index").setSource("field", "value").get();
        refresh("test-priority-index");
        SearchResponse response = client().prepareSearch("test-priority-index").get();
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));

        logger.info("Template priority test completed - high priority template applied");
    }

    /**
     * Tests composable (v2) index template with encryption.
     * Composable templates are more flexible and support component templates.
     */
    public void testComposableIndexTemplate() throws Exception {
        logger.info("Testing composable index template");

        // Create composable template
        Template template = new Template(
            Settings.builder().put(cryptoSettings()).put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(),
            null,
            null
        );

        ComposableIndexTemplate composableTemplate = new ComposableIndexTemplate(
            Arrays.asList("composable-*"),
            template,
            null,
            100L,
            null,
            null,
            null
        );

        client()
            .execute(
                org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.INSTANCE,
                new org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.Request("composable-template")
                    .indexTemplate(composableTemplate)
            )
            .get();

        // Create index from composable template
        client().admin().indices().prepareCreate("composable-index-001").get();
        ensureGreen("composable-index-001");

        // Verify encryption settings
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings("composable-index-001").get();
        assertThat(settingsResponse.getSetting("composable-index-001", "index.store.type"), equalTo("cryptofs"));

        // Test encryption
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("composable-index-001").setSource("value", i).get();
        }
        refresh("composable-index-001");

        SearchResponse response = client().prepareSearch("composable-index-001").setSize(0).get();
        assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs));

        logger.info("Composable template test completed");
    }

    /**
     * Tests composable template with multiple settings layers.
     * Simulates component template behavior with combined settings.
     */
    public void testComponentTemplates() throws Exception {
        logger.info("Testing composable template with layered settings");

        // Create composable template with combined encryption + index settings
        // This simulates what component templates would do when combined
        Template combinedTemplate = new Template(
            Settings.builder().put(cryptoSettings()).put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(),
            null,
            null
        );

        ComposableIndexTemplate finalTemplate = new ComposableIndexTemplate(
            Arrays.asList("component-test-*"),
            combinedTemplate,
            null, // component list - skipped for compatibility
            200L,
            null,
            null,
            null
        );

        client()
            .execute(
                org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.INSTANCE,
                new org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.Request("combined-template")
                    .indexTemplate(finalTemplate)
            )
            .get();

        // Create index - should inherit combined settings
        client().admin().indices().prepareCreate("component-test-index").get();
        ensureGreen("component-test-index");

        // Verify both settings applied
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings("component-test-index").get();
        assertThat(settingsResponse.getSetting("component-test-index", "index.store.type"), equalTo("cryptofs"));
        assertThat(settingsResponse.getSetting("component-test-index", "index.number_of_shards"), equalTo("1"));

        // Test encryption
        client().prepareIndex("component-test-index").setSource("test", "data").get();
        refresh("component-test-index");
        SearchResponse response = client().prepareSearch("component-test-index").get();
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));

        logger.info("Layered settings template test completed");
    }

    /**
     * Tests index rollover with template encryption.
     * Rollover creates new indices that should inherit template settings.
     */
    public void testIndexRolloverWithTemplate() throws Exception {
        logger.info("Testing index rollover with encrypted template");

        // Create template for rollover indices
        Settings templateSettings = Settings
            .builder()
            .put(cryptoSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("rollover-template")
            .setPatterns(Arrays.asList("rollover-*"))
            .setSettings(templateSettings)
            .get();

        // Create initial index with alias
        client().admin().indices().prepareCreate("rollover-000001").addAlias(new Alias("rollover-alias").writeIndex(true)).get();
        ensureGreen("rollover-000001");

        // Verify initial index has encryption
        GetSettingsResponse initialSettings = client().admin().indices().prepareGetSettings("rollover-000001").get();
        assertThat(initialSettings.getSetting("rollover-000001", "index.store.type"), equalTo("cryptofs"));

        // Index some documents
        int docsBeforeRollover = randomIntBetween(10, 30);
        for (int i = 0; i < docsBeforeRollover; i++) {
            client().prepareIndex("rollover-alias").setSource("value", i).get();
        }
        refresh("rollover-alias");

        // Perform rollover based on document count
        RolloverRequest rolloverRequest = new RolloverRequest("rollover-alias", "rollover-000002");
        rolloverRequest.addMaxIndexDocsCondition(5L); // Force rollover
        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(rolloverRequest).get();

        assertTrue("Rollover should succeed", rolloverResponse.isRolledOver());
        assertThat("New index should be created", rolloverResponse.getNewIndex(), equalTo("rollover-000002"));

        ensureGreen("rollover-000002");

        // Verify new index has encryption from template
        GetSettingsResponse newIndexSettings = client().admin().indices().prepareGetSettings("rollover-000002").get();
        assertThat(newIndexSettings.getSetting("rollover-000002", "index.store.type"), equalTo("cryptofs"));
        assertThat(newIndexSettings.getSetting("rollover-000002", "index.store.crypto.key_provider"), equalTo("dummy"));

        // Index more documents to new index
        int docsAfterRollover = randomIntBetween(10, 30);
        for (int i = 0; i < docsAfterRollover; i++) {
            client().prepareIndex("rollover-alias").setSource("value", i + 1000).get();
        }
        refresh("rollover-alias");

        // Verify both indices are encrypted and searchable
        SearchResponse oldIndexResponse = client().prepareSearch("rollover-000001").setSize(0).get();
        SearchResponse newIndexResponse = client().prepareSearch("rollover-000002").setSize(0).get();

        assertThat("Old index should have docs", oldIndexResponse.getHits().getTotalHits().value(), greaterThan(0L));
        assertThat("New index should have docs", newIndexResponse.getHits().getTotalHits().value(), greaterThan(0L));

        // Search across alias
        SearchResponse aliasResponse = client().prepareSearch("rollover-alias").setSize(0).get();
        assertThat(
            "Alias should search both indices",
            aliasResponse.getHits().getTotalHits().value(),
            equalTo(oldIndexResponse.getHits().getTotalHits().value() + newIndexResponse.getHits().getTotalHits().value())
        );

        logger.info("Rollover test completed - old: {}, new: {}", "rollover-000001", "rollover-000002");
    }

    /**
     * Tests multiple rollovers to ensure template consistently applies.
     */
    public void testMultipleRollovers() throws Exception {
        logger.info("Testing multiple consecutive rollovers");

        Settings templateSettings = Settings.builder().put(cryptoSettings()).put("index.number_of_shards", 1).build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("multi-rollover-template")
            .setPatterns(Arrays.asList("multi-rollover-*"))
            .setSettings(templateSettings)
            .get();

        // Create initial index
        client()
            .admin()
            .indices()
            .prepareCreate("multi-rollover-000001")
            .addAlias(new Alias("multi-rollover-alias").writeIndex(true))
            .get();

        String[] rolledIndices = new String[5];
        rolledIndices[0] = "multi-rollover-000001";

        // Index initial docs to first index
        for (int j = 0; j < 10; j++) {
            client().prepareIndex("multi-rollover-alias").setSource("rollover", 0, "doc", j).get();
        }
        refresh("multi-rollover-alias");

        // Perform multiple rollovers
        for (int i = 1; i < 5; i++) {
            String newIndexName = String.format(Locale.ROOT, "multi-rollover-%06d", i + 1);

            // Rollover
            RolloverRequest rolloverRequest = new RolloverRequest("multi-rollover-alias", newIndexName);
            rolloverRequest.addMaxIndexDocsCondition(5L);
            RolloverResponse response = client().admin().indices().rolloverIndex(rolloverRequest).get();

            assertTrue("Rollover " + i + " should succeed", response.isRolledOver());
            rolledIndices[i] = newIndexName;
            ensureGreen(newIndexName);

            // Index docs to new index for next rollover (except last one)
            if (i < 4) {
                for (int j = 0; j < 10; j++) {
                    client().prepareIndex("multi-rollover-alias").setSource("rollover", i, "doc", j).get();
                }
                refresh("multi-rollover-alias");
            }
        }

        // Verify all indices have encryption and are searchable
        for (int i = 0; i < rolledIndices.length; i++) {
            String index = rolledIndices[i];
            GetSettingsResponse settings = client().admin().indices().prepareGetSettings(index).get();
            assertThat("Index " + index + " should be encrypted", settings.getSetting(index, "index.store.type"), equalTo("cryptofs"));

            // Only verify docs for indices that have data (first 4 indices)
            if (i < 4) {
                SearchResponse searchResponse = client().prepareSearch(index).setSize(0).get();
                assertThat("Index " + index + " should have docs", searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
            }
        }

        logger.info("Multiple rollovers test completed - {} indices created", rolledIndices.length);
    }

    /**
     * Tests rollover based on index age (time-based).
     * Simulates aging by creating an index and waiting for a short period.
     */
    public void testRolloverByAge() throws Exception {
        logger.info("Testing age-based rollover with encryption");

        Settings templateSettings = Settings
            .builder()
            .put(cryptoSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("age-rollover-template")
            .setPatterns(Arrays.asList("age-test-*"))
            .setSettings(templateSettings)
            .get();

        // Create initial index
        client().admin().indices().prepareCreate("age-test-000001").addAlias(new Alias("age-test-alias").writeIndex(true)).get();
        ensureGreen("age-test-000001");

        // Verify encryption
        GetSettingsResponse settings1 = client().admin().indices().prepareGetSettings("age-test-000001").get();
        assertThat(settings1.getSetting("age-test-000001", "index.store.type"), equalTo("cryptofs"));

        // Index documents
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("age-test-alias").setSource("timestamp", System.currentTimeMillis(), "value", i).get();
        }
        refresh("age-test-alias");

        // Wait a bit to age the index
        Thread.sleep(2000); // 2 seconds

        // Attempt rollover with age condition (1 second - should trigger)
        RolloverRequest rolloverRequest = new RolloverRequest("age-test-alias", "age-test-000002");
        rolloverRequest.addMaxIndexAgeCondition(TimeValue.timeValueSeconds(1));
        RolloverResponse response = client().admin().indices().rolloverIndex(rolloverRequest).get();

        assertTrue("Rollover should succeed based on age", response.isRolledOver());
        assertThat("New index should be created", response.getNewIndex(), equalTo("age-test-000002"));
        ensureGreen("age-test-000002");

        // Verify new index has encryption
        GetSettingsResponse settings2 = client().admin().indices().prepareGetSettings("age-test-000002").get();
        assertThat(settings2.getSetting("age-test-000002", "index.store.type"), equalTo("cryptofs"));

        // Index to new index
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("age-test-alias").setSource("timestamp", System.currentTimeMillis(), "value", i + 100).get();
        }
        refresh("age-test-alias");

        // Verify both indices work
        SearchResponse oldResponse = client().prepareSearch("age-test-000001").setSize(0).get();
        SearchResponse newResponse = client().prepareSearch("age-test-000002").setSize(0).get();
        assertThat(oldResponse.getHits().getTotalHits().value(), equalTo(10L));
        assertThat(newResponse.getHits().getTotalHits().value(), equalTo(5L));

        logger.info("Age-based rollover test completed");
    }

    /**
     * Tests rollover with continuous data ingestion simulation.
     * This simulates a real-world scenario where data is continuously indexed
     * and rollover happens based on document count threshold.
     */
    public void testRolloverContinuousIngestion() throws Exception {
        logger.info("Testing rollover with continuous data ingestion");

        Settings templateSettings = Settings
            .builder()
            .put(cryptoSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("ingestion-rollover-template")
            .setPatterns(Arrays.asList("logs-ingestion-*"))
            .setSettings(templateSettings)
            .get();

        // Create initial index
        client()
            .admin()
            .indices()
            .prepareCreate("logs-ingestion-000001")
            .addAlias(new Alias("logs-ingestion-write").writeIndex(true))
            .get();
        ensureGreen("logs-ingestion-000001");

        // Verify encryption
        GetSettingsResponse settings1 = client().admin().indices().prepareGetSettings("logs-ingestion-000001").get();
        assertThat(settings1.getSetting("logs-ingestion-000001", "index.store.type"), equalTo("cryptofs"));

        // Simulate continuous ingestion with multiple rollover cycles
        int totalDocs = 0;
        String currentIndexPattern = "logs-ingestion-";

        for (int cycle = 1; cycle <= 3; cycle++) {
            logger.info("Ingestion cycle {}", cycle);

            // Index documents until rollover threshold
            for (int i = 0; i < 25; i++) {
                client()
                    .prepareIndex("logs-ingestion-write")
                    .setSource("cycle", cycle, "doc", i, "timestamp", System.currentTimeMillis(), "message", "log entry " + totalDocs)
                    .get();
                totalDocs++;
            }
            refresh("logs-ingestion-write");

            // Trigger rollover when we have enough docs
            String newIndexName = String.format(Locale.ROOT, "logs-ingestion-%06d", cycle + 1);
            RolloverRequest rolloverRequest = new RolloverRequest("logs-ingestion-write", newIndexName);
            rolloverRequest.addMaxIndexDocsCondition(20L); // Rollover after 20 docs
            RolloverResponse response = client().admin().indices().rolloverIndex(rolloverRequest).get();

            assertTrue("Rollover should succeed in cycle " + cycle, response.isRolledOver());
            ensureGreen(newIndexName);

            // Verify new index has encryption
            GetSettingsResponse newSettings = client().admin().indices().prepareGetSettings(newIndexName).get();
            assertThat(
                "New index in cycle " + cycle + " should be encrypted",
                newSettings.getSetting(newIndexName, "index.store.type"),
                equalTo("cryptofs")
            );
        }

        // Verify all data is searchable across all rolled-over indices
        SearchResponse totalResponse = client().prepareSearch("logs-ingestion-*").setSize(0).get();
        assertThat("All ingested docs should be searchable", totalResponse.getHits().getTotalHits().value(), equalTo((long) totalDocs));

        logger.info("Continuous ingestion rollover test completed - {} total docs across 4 indices", totalDocs);
    }

    /**
     * Tests combined rollover conditions (docs OR age OR size).
     * Any condition being met triggers rollover.
     */
    public void testRolloverMultipleConditions() throws Exception {
        logger.info("Testing rollover with multiple conditions");

        Settings templateSettings = Settings
            .builder()
            .put(cryptoSettings())
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("multi-condition-template")
            .setPatterns(Arrays.asList("multi-cond-*"))
            .setSettings(templateSettings)
            .get();

        // Create initial index
        client().admin().indices().prepareCreate("multi-cond-000001").addAlias(new Alias("multi-cond-alias").writeIndex(true)).get();
        ensureGreen("multi-cond-000001");

        // Index just a few documents (below doc count threshold)
        for (int i = 0; i < 3; i++) {
            client().prepareIndex("multi-cond-alias").setSource("value", i).get();
        }
        refresh("multi-cond-alias");

        // Wait for age condition
        Thread.sleep(1500);

        // Rollover with multiple conditions: 100 docs OR 1 second age
        // Age condition should trigger (docs won't)
        RolloverRequest rolloverRequest = new RolloverRequest("multi-cond-alias", "multi-cond-000002");
        rolloverRequest.addMaxIndexDocsCondition(100L); // Not met
        rolloverRequest.addMaxIndexAgeCondition(TimeValue.timeValueSeconds(1)); // MET

        RolloverResponse response = client().admin().indices().rolloverIndex(rolloverRequest).get();

        assertTrue("Rollover should succeed when ANY condition is met", response.isRolledOver());
        assertThat("New index should be created", response.getNewIndex(), equalTo("multi-cond-000002"));
        ensureGreen("multi-cond-000002");

        // Verify encryption on new index
        GetSettingsResponse settings = client().admin().indices().prepareGetSettings("multi-cond-000002").get();
        assertThat(settings.getSetting("multi-cond-000002", "index.store.type"), equalTo("cryptofs"));

        logger.info("Multiple conditions rollover test completed");
    }

    /**
     * Tests template update and re-application.
     */
    public void testTemplateUpdate() throws Exception {
        logger.info("Testing template update");

        // Create initial template
        Settings initialSettings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "initial-key")
            .put("index.number_of_shards", 1)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("update-template")
            .setPatterns(Arrays.asList("update-test-*"))
            .setSettings(initialSettings)
            .get();

        // Create index with initial template
        client().admin().indices().prepareCreate("update-test-001").get();
        ensureGreen("update-test-001");

        GetSettingsResponse settings1 = client().admin().indices().prepareGetSettings("update-test-001").get();
        assertThat(settings1.getSetting("update-test-001", "index.store.crypto.kms.key_arn"), equalTo("initial-key"));

        // Update template with new key
        Settings updatedSettings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "updated-key")
            .put("index.number_of_shards", 2)
            .build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("update-template")
            .setPatterns(Arrays.asList("update-test-*"))
            .setSettings(updatedSettings)
            .get();

        // Create new index - should use updated template
        client().admin().indices().prepareCreate("update-test-002").get();
        ensureGreen("update-test-002");

        GetSettingsResponse settings2 = client().admin().indices().prepareGetSettings("update-test-002").get();
        assertThat(settings2.getSetting("update-test-002", "index.store.crypto.kms.key_arn"), equalTo("updated-key"));
        assertThat(settings2.getSetting("update-test-002", "index.number_of_shards"), equalTo("2"));

        // Verify old index unchanged
        settings1 = client().admin().indices().prepareGetSettings("update-test-001").get();
        assertThat(settings1.getSetting("update-test-001", "index.store.crypto.kms.key_arn"), equalTo("initial-key"));

        // Verify both indices work with encryption
        client().prepareIndex("update-test-001").setSource("test", "old").get();
        client().prepareIndex("update-test-002").setSource("test", "new").get();
        refresh("update-test-001", "update-test-002");

        SearchResponse response1 = client().prepareSearch("update-test-001").get();
        SearchResponse response2 = client().prepareSearch("update-test-002").get();
        assertThat(response1.getHits().getTotalHits().value(), greaterThan(0L));
        assertThat(response2.getHits().getTotalHits().value(), greaterThan(0L));

        logger.info("Template update test completed");
    }

    /**
     * Tests template deletion and index creation without template.
     */
    public void testTemplateDeletion() throws Exception {
        logger.info("Testing template deletion");

        Settings templateSettings = Settings.builder().put(cryptoSettings()).put("index.number_of_shards", 1).build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("delete-template")
            .setPatterns(Arrays.asList("delete-test-*"))
            .setSettings(templateSettings)
            .get();

        // Create index with template
        client().admin().indices().prepareCreate("delete-test-001").get();
        ensureGreen("delete-test-001");

        GetSettingsResponse settings1 = client().admin().indices().prepareGetSettings("delete-test-001").get();
        assertThat(settings1.getSetting("delete-test-001", "index.store.type"), equalTo("cryptofs"));

        // Delete template
        client().admin().indices().deleteTemplate(new DeleteIndexTemplateRequest("delete-template")).get();

        // Create new index - should not have template settings (will use default)
        client().admin().indices().prepareCreate("delete-test-002").get();
        ensureGreen("delete-test-002");

        GetSettingsResponse settings2 = client().admin().indices().prepareGetSettings("delete-test-002").get();
        // Without template, index should use default store type (not cryptofs from template)
        String storeType = settings2.getSetting("delete-test-002", "index.store.type");
        // Either null (using implicit default) or an explicit default type, but NOT cryptofs
        assertTrue("Index should not use cryptofs after template deletion", storeType == null || !storeType.equals("cryptofs"));

        // Verify old index still works with encryption
        client().prepareIndex("delete-test-001").setSource("test", "value").get();
        refresh("delete-test-001");
        SearchResponse response1 = client().prepareSearch("delete-test-001").get();
        assertThat(response1.getHits().getTotalHits().value(), greaterThan(0L));

        logger.info("Template deletion test completed");
    }

    /**
     * Tests template with alias definitions.
     */
    public void testTemplateWithAliases() throws Exception {
        logger.info("Testing template with aliases");

        Settings templateSettings = Settings.builder().put(cryptoSettings()).put("index.number_of_shards", 1).build();

        client()
            .admin()
            .indices()
            .preparePutTemplate("alias-template")
            .setPatterns(Arrays.asList("alias-test-*"))
            .setSettings(templateSettings)
            .addAlias(new Alias("read-alias"))
            .addAlias(new Alias("write-alias").writeIndex(true))
            .get();

        // Create index
        client().admin().indices().prepareCreate("alias-test-001").get();
        ensureGreen("alias-test-001");

        // Verify encryption
        GetSettingsResponse settings = client().admin().indices().prepareGetSettings("alias-test-001").get();
        assertThat(settings.getSetting("alias-test-001", "index.store.type"), equalTo("cryptofs"));

        // Test writing through alias
        client().prepareIndex("write-alias").setSource("field", "value").get();
        refresh("alias-test-001");

        // Test reading through alias
        SearchResponse readResponse = client().prepareSearch("read-alias").get();
        assertThat(readResponse.getHits().getTotalHits().value(), equalTo(1L));

        SearchResponse writeResponse = client().prepareSearch("write-alias").get();
        assertThat(writeResponse.getHits().getTotalHits().value(), equalTo(1L));

        logger.info("Template with aliases test completed");
    }

    /**
     * Tests date math patterns in template.
     */
    public void testTemplateDateMathPattern() throws Exception {
        logger.info("Testing template with date math pattern simulation");

        Settings templateSettings = Settings.builder().put(cryptoSettings()).build();

        // Create template for time-based indices
        client()
            .admin()
            .indices()
            .preparePutTemplate("timebased-template")
            .setPatterns(Arrays.asList("logs-*-*"))
            .setSettings(templateSettings)
            .get();

        // Simulate date-based index names (must be lowercase)
        String[] dateIndices = { "logs-2024-01", "logs-2024-02", "logs-2024-03", "logs-2024-q1", "logs-2024-q2" };

        for (String indexName : dateIndices) {
            client().admin().indices().prepareCreate(indexName).get();
        }
        ensureGreen(dateIndices);

        // Verify all have encryption
        for (String indexName : dateIndices) {
            GetSettingsResponse settings = client().admin().indices().prepareGetSettings(indexName).get();
            assertThat(indexName + " should be encrypted", settings.getSetting(indexName, "index.store.type"), equalTo("cryptofs"));

            // Index data
            client().prepareIndex(indexName).setSource("timestamp", System.currentTimeMillis()).get();
        }
        refresh(dateIndices);

        // Verify all are searchable
        SearchResponse response = client().prepareSearch("logs-*").setSize(0).get();
        assertThat(response.getHits().getTotalHits().value(), equalTo((long) dateIndices.length));

        logger.info("Date math pattern test completed - {} indices", dateIndices.length);
    }

    /**
     * Tests template with different shard configurations.
     */
    public void testTemplateShardConfiguration() throws Exception {
        logger.info("Testing template with various shard configurations");

        // Single shard template with high priority to override random template
        client()
            .admin()
            .indices()
            .preparePutTemplate("single-shard-template")
            .setPatterns(Arrays.asList("single-*"))
            .setSettings(
                Settings.builder().put(cryptoSettings()).put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build()
            )
            .setOrder(100) // High priority to override defaults
            .get();

        // Multi-shard template with high priority
        client()
            .admin()
            .indices()
            .preparePutTemplate("multi-shard-template")
            .setPatterns(Arrays.asList("multi-*"))
            .setSettings(
                Settings.builder().put(cryptoSettings()).put("index.number_of_shards", 3).put("index.number_of_replicas", 0).build()
            )
            .setOrder(100) // High priority to override defaults
            .get();

        // Create indices
        client().admin().indices().prepareCreate("single-test-index").get();
        client().admin().indices().prepareCreate("multi-test-index").get();
        ensureGreen("single-test-index", "multi-test-index");

        // Verify shard configuration
        GetSettingsResponse singleSettings = client().admin().indices().prepareGetSettings("single-test-index").get();
        assertThat(
            "Single shard index should have 1 shard",
            singleSettings.getSetting("single-test-index", "index.number_of_shards"),
            equalTo("1")
        );
        assertThat(singleSettings.getSetting("single-test-index", "index.store.type"), equalTo("cryptofs"));

        GetSettingsResponse multiSettings = client().admin().indices().prepareGetSettings("multi-test-index").get();
        assertThat(
            "Multi shard index should have 3 shards",
            multiSettings.getSetting("multi-test-index", "index.number_of_shards"),
            equalTo("3")
        );
        assertThat(multiSettings.getSetting("multi-test-index", "index.store.type"), equalTo("cryptofs"));

        // Index and search data in both
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("single-test-index").setSource("value", i).get();
            client().prepareIndex("multi-test-index").setSource("value", i).get();
        }
        refresh("single-test-index", "multi-test-index");

        SearchResponse singleResponse = client().prepareSearch("single-test-index").setSize(0).get();
        SearchResponse multiResponse = client().prepareSearch("multi-test-index").setSize(0).get();

        assertThat(singleResponse.getHits().getTotalHits().value(), equalTo(100L));
        assertThat(multiResponse.getHits().getTotalHits().value(), equalTo(100L));

        logger.info("Shard configuration test completed");
    }

    // ==================== Enable/Disable Flag Integration Tests ====================

    /**
     * Tests that when plugin is disabled, cryptofs store type cannot be used.
     * This verifies the disable flag prevents encryption operations.
     */
    public void testPluginDisabledPreventsCryptofsIndices() throws Exception {
        logger.info("Testing that disabled plugin prevents cryptofs indices");

        // Note: This test assumes the plugin is enabled in the cluster setup
        // In a real scenario with disabled plugin, creating a cryptofs index would fail
        // Since we can't dynamically disable the plugin (it's a FINAL setting),
        // we test that the plugin properly reports its state

        // Create a disabled plugin instance to verify its behavior
        Settings disabledSettings = Settings.builder().put(CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED, false).build();

        CryptoDirectoryPlugin disabledPlugin = new CryptoDirectoryPlugin(disabledSettings);
        assertTrue("Plugin should report as disabled", disabledPlugin.isDisabled());
        assertTrue("Disabled plugin should not register directory factories", disabledPlugin.getDirectoryFactories().isEmpty());

        logger.info("Disabled plugin test completed");
    }

    /**
     * Tests that the enabled setting is properly configured with correct properties.
     */
    public void testEnabledSettingConfiguration() throws Exception {
        logger.info("Testing enabled setting configuration");

        CryptoDirectoryPlugin plugin = new CryptoDirectoryPlugin(Settings.EMPTY);

        // Verify setting is included
        boolean hasEnabledSetting = plugin
            .getSettings()
            .stream()
            .anyMatch(s -> s.getKey().equals(CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED));

        assertTrue("Plugin settings should include enabled setting", hasEnabledSetting);

        // Verify default value (false = disabled by default)
        assertEquals(
            "Enabled setting should default to false (disabled by default)",
            Boolean.FALSE,
            CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED_SETTING.getDefault(Settings.EMPTY)
        );

        logger.info("Enabled setting configuration test completed");
    }

    /**
     * Tests that enabled plugin properly creates encrypted indices.
     */
    public void testEnabledPluginCreatesEncryptedIndices() throws Exception {
        logger.info("Testing that enabled plugin creates encrypted indices");

        // Create index with cryptofs (plugin must be explicitly enabled in test cluster)
        Settings indexSettings = Settings
            .builder()
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "dummy")
            .put("index.store.crypto.kms.key_arn", "test-enabled-key")
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        client().admin().indices().prepareCreate("enabled-test-index").setSettings(indexSettings).get();
        ensureGreen("enabled-test-index");

        // Verify encryption is active
        GetSettingsResponse settings = client().admin().indices().prepareGetSettings("enabled-test-index").get();
        assertThat("Index should use cryptofs", settings.getSetting("enabled-test-index", "index.store.type"), equalTo("cryptofs"));

        // Verify encryption works by indexing and retrieving data
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("enabled-test-index").setSource("enabled_test", true, "doc_id", i).get();
        }
        refresh("enabled-test-index");

        SearchResponse response = client().prepareSearch("enabled-test-index").setSize(0).get();
        assertThat("All documents should be retrievable", response.getHits().getTotalHits().value(), equalTo((long) numDocs));

        logger.info("Enabled plugin test completed - {} docs indexed and retrieved", numDocs);
    }

    /**
     * Tests plugin state transitions and behavior consistency.
     */
    public void testPluginStateConsistency() throws Exception {
        logger.info("Testing plugin state consistency");

        // Test enabled state
        Settings enabledSettings = Settings.builder().put(CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED, true).build();
        CryptoDirectoryPlugin enabledPlugin = new CryptoDirectoryPlugin(enabledSettings);

        assertFalse("Plugin should not be disabled when enabled", enabledPlugin.isDisabled());
        assertFalse("Enabled plugin should register factories", enabledPlugin.getDirectoryFactories().isEmpty());
        assertNotNull("Enabled plugin should provide cryptofs factory", enabledPlugin.getDirectoryFactories().get("cryptofs"));

        // Test disabled state
        Settings disabledSettings = Settings.builder().put(CryptoDirectoryPlugin.CRYPTO_PLUGIN_ENABLED, false).build();
        CryptoDirectoryPlugin disabledPlugin = new CryptoDirectoryPlugin(disabledSettings);

        assertTrue("Plugin should be disabled", disabledPlugin.isDisabled());
        assertTrue("Disabled plugin should not register factories", disabledPlugin.getDirectoryFactories().isEmpty());

        // Test default state (should be disabled by default)
        CryptoDirectoryPlugin defaultPlugin = new CryptoDirectoryPlugin(Settings.EMPTY);
        assertTrue("Plugin should be disabled by default", defaultPlugin.isDisabled());

        logger.info("Plugin state consistency test completed");
    }
}

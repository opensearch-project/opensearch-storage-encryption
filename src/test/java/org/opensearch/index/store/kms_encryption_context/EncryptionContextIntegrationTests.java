/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.kms_encryption_context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;

/**
 * Integration tests for encryption context resolution.
 * These tests verify the end-to-end behavior of resolvers with mocked cluster metadata.
 */
public class EncryptionContextIntegrationTests extends LuceneTestCase {

    private static final String AMAZON_ENC_CTX_VALUE = "domainARN=arn:aws:es:eu-west-1:110365260509:domain/test-domain";

    /**
     * Test that Amazon resolver finds encryption context among multiple repositories.
     */
    public void testAmazonResolverFindsCorrectRepositoryAmongMultiple() {
        // Create multiple repositories, only one with encryption context
        Settings repo1Settings = Settings.builder().put("type", "s3").put("bucket", "bucket1").build();

        Settings repo2Settings = Settings.builder().put("type", "s3").put("bucket", "bucket2").build();

        Settings repo3Settings = Settings
            .builder()
            .put("type", "s3")
            .put("amazon_es_kms_enc_ctx", AMAZON_ENC_CTX_VALUE)
            .put("bucket", "bucket3")
            .build();

        RepositoryMetadata repo1 = new RepositoryMetadata("repo-1", "s3", repo1Settings);
        RepositoryMetadata repo2 = new RepositoryMetadata("repo-2", "s3", repo2Settings);
        RepositoryMetadata repo3 = new RepositoryMetadata("repo-with-enc-ctx", "s3", repo3Settings);

        RepositoriesMetadata repositoriesMetadata = new RepositoriesMetadata(java.util.Arrays.asList(repo1, repo2, repo3));

        ClusterService cs = createMockClusterService(repositoriesMetadata);

        // Create AMAZON resolver
        EncryptionContextResolver resolver = EncryptionContextResolverFactory.create(EncryptionContextResolverType.AMAZON, cs);

        String encCtx = resolver.resolveDefaultEncryptionContext();
        assertEquals("Resolver should find encryption context from repo-with-enc-ctx", AMAZON_ENC_CTX_VALUE, encCtx);
    }

    /**
     * Test that Amazon resolver returns first matching repository when multiple have encryption context.
     */
    public void testAmazonResolverReturnsFirstMatchingRepository() {
        String firstEncCtx = "domainARN=arn:aws:es:us-east-1:111:domain/first";
        String secondEncCtx = "domainARN=arn:aws:es:us-west-2:222:domain/second";

        Settings repo1Settings = Settings.builder().put("type", "s3").put("amazon_es_kms_enc_ctx", firstEncCtx).build();

        Settings repo2Settings = Settings.builder().put("type", "s3").put("amazon_es_kms_enc_ctx", secondEncCtx).build();

        RepositoryMetadata repo1 = new RepositoryMetadata("first-repo", "s3", repo1Settings);
        RepositoryMetadata repo2 = new RepositoryMetadata("second-repo", "s3", repo2Settings);

        RepositoriesMetadata repositoriesMetadata = new RepositoriesMetadata(java.util.Arrays.asList(repo1, repo2));

        ClusterService cs = createMockClusterService(repositoriesMetadata);

        EncryptionContextResolver resolver = EncryptionContextResolverFactory.create(EncryptionContextResolverType.AMAZON, cs);

        String encCtx = resolver.resolveDefaultEncryptionContext();
        assertEquals("Resolver should return encryption context from first matching repository", firstEncCtx, encCtx);
    }

    /**
     * Test Amazon resolver when no repositories exist.
     */
    public void testAmazonResolverWithNoRepositories() {
        ClusterService csNoRepos = createMockClusterService(null);

        EncryptionContextResolver resolver = EncryptionContextResolverFactory.create(EncryptionContextResolverType.AMAZON, csNoRepos);

        String encCtx = resolver.resolveDefaultEncryptionContext();
        assertEquals("Resolver should return empty when no repositories exist", "", encCtx);
    }

    /**
     * Test NONE resolver always returns empty regardless of cluster state.
     */
    public void testNoneResolverAlwaysReturnsEmpty() {
        // Create repository with encryption context
        Settings repoSettings = Settings.builder().put("type", "s3").put("amazon_es_kms_enc_ctx", AMAZON_ENC_CTX_VALUE).build();

        RepositoryMetadata repo = new RepositoryMetadata("test-repo", "s3", repoSettings);

        RepositoriesMetadata repositoriesMetadata = new RepositoriesMetadata(Collections.singletonList(repo));

        ClusterService cs = createMockClusterService(repositoriesMetadata);

        // Create NONE resolver
        EncryptionContextResolver resolver = EncryptionContextResolverFactory.create(EncryptionContextResolverType.NONE, cs);

        String encCtx = resolver.resolveDefaultEncryptionContext();
        assertEquals("NONE resolver should return empty even when repository has encryption context", "", encCtx);
    }

    /**
     * Test that factory creates correct resolver type.
     */
    public void testFactoryCreatesCorrectResolverTypes() {
        ClusterService cs = mock(ClusterService.class);

        // Test AMAZON type
        EncryptionContextResolver amazonResolver = EncryptionContextResolverFactory.create(EncryptionContextResolverType.AMAZON, cs);
        assertTrue("Factory should create AmazonEncryptionContextResolver", amazonResolver instanceof AmazonEncryptionContextResolver);
        assertEquals("AmazonEncryptionContextResolver", amazonResolver.getName());

        // Test NONE type
        EncryptionContextResolver noneResolver = EncryptionContextResolverFactory.create(EncryptionContextResolverType.NONE, cs);
        assertTrue("Factory should create NoOpEncryptionContextResolver", noneResolver instanceof NoOpEncryptionContextResolver);
        assertEquals("NoOpEncryptionContextResolver", noneResolver.getName());
    }

    /**
     * Helper method to create a mock ClusterService.
     */
    private ClusterService createMockClusterService(RepositoriesMetadata repositoriesMetadata) {
        ClusterService mockCS = mock(ClusterService.class);
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);

        when(mockCS.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.custom(RepositoriesMetadata.TYPE)).thenReturn(repositoriesMetadata);

        return mockCS;
    }
}

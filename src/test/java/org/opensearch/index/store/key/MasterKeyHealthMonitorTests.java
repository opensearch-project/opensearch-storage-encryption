/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.key;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.key.MasterKeyHealthMonitor.FailureState;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

/**
 * Unit tests for MasterKeyHealthMonitor focusing on failure reporting,
 * error classification, block management, and state transitions.
 * 
 */
public class MasterKeyHealthMonitorTests extends OpenSearchTestCase {

    @Mock
    private Client mockClient;

    @Mock
    private ClusterService mockClusterService;

    @Mock
    private ClusterState mockClusterState;

    @Mock
    private Metadata mockMetadata;

    @Mock
    private IndexMetadata mockIndexMetadata;

    @Mock
    private AdminClient mockAdminClient;

    @Mock
    private IndicesAdminClient mockIndicesAdminClient;

    @SuppressWarnings("unchecked")
    @Mock
    private ActionFuture<AcknowledgedResponse> mockFuture;

    private MasterKeyHealthMonitor monitor;

    private static final String TEST_INDEX_UUID = "test-index-uuid-123";
    private static final String TEST_INDEX_NAME = "test-index";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Setup mock cluster service chain
        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.index(TEST_INDEX_NAME)).thenReturn(mockIndexMetadata);

        // Setup mock client chain for block operations
        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);
        when(mockIndicesAdminClient.updateSettings(any(UpdateSettingsRequest.class))).thenReturn(mockFuture);
        when(mockFuture.actionGet()).thenReturn(mock(AcknowledgedResponse.class));

        // Initialize monitor without starting background thread
        Settings settings = Settings.builder().put("node.store.crypto.key_refresh_interval", "3600s").build();
        MasterKeyHealthMonitor.initialize(settings, mockClient, mockClusterService);
        monitor = MasterKeyHealthMonitor.getInstance();
    }

    @After
    public void tearDown() throws Exception {
        MasterKeyHealthMonitor.reset();
        super.tearDown();
    }

    /**
     * Helper to get the failure tracker via reflection
     */
    @SuppressForbidden(reason = "Test needs to access private failure tracker")
    private ConcurrentHashMap<String, FailureState> getFailureTracker() throws Exception {
        Field trackerField = MasterKeyHealthMonitor.class.getDeclaredField("failureTracker");
        trackerField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, FailureState> tracker = (ConcurrentHashMap<String, FailureState>) trackerField.get(monitor);
        return tracker;
    }

    /**
     * Transient errors (THROTTLING) should NOT apply blocks
     */
    public void testTransientErrorDoesNotApplyBlocks() throws Exception {
        // Setup: Index has no blocks initially
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        Exception transientError = new RuntimeException("ThrottlingException: Rate exceeded");

        // Act: Report transient failure
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, transientError, FailureType.TRANSIENT);

        // Assert: No blocks applied
        verify(mockIndicesAdminClient, never()).updateSettings(any());

        // Verify failure tracked but blocksApplied=false
        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        assertNotNull("Failure should be tracked", state);
        assertFalse("Blocks should not be applied for transient error", state.blocksApplied);
        assertEquals("Failure type should be TRANSIENT", FailureType.TRANSIENT, state.failureType);
    }

    /**
     * Critical errors (DISABLED_KEY) should apply blocks
     */
    public void testCriticalErrorAppliesBlocks() throws Exception {
        // Setup: Index has no blocks initially
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        Exception criticalError = new RuntimeException("DisabledException: Key is disabled");

        // Act: Report critical failure
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, criticalError, FailureType.CRITICAL);

        // Assert: Verify failure tracked with blocksApplied=true
        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        assertNotNull("Failure should be tracked", state);
        assertTrue("Blocks should be applied for critical error", state.blocksApplied);
        assertEquals("Failure type should be CRITICAL", FailureType.CRITICAL, state.failureType);
    }

    /**
     * Multiple transient failures should NOT escalate to blocks
     */
    public void testMultipleTransientFailuresNoBlocks() throws Exception {
        // Setup
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        Exception transientError = new RuntimeException("ThrottlingException: Rate exceeded");

        // Act: Report multiple transient failures
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, transientError, FailureType.TRANSIENT);
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, transientError, FailureType.TRANSIENT);
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, transientError, FailureType.TRANSIENT);

        Thread.sleep(100);

        // Assert: Still no blocks
        verify(mockIndicesAdminClient, never()).updateSettings(any());

        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        assertFalse("Blocks should not be applied after multiple transient failures", state.blocksApplied);
        assertEquals("Failure type should remain TRANSIENT", FailureType.TRANSIENT, state.failureType);
    }

    /**
     * Error escalation from TRANSIENT to CRITICAL applies blocks
     */
    public void testErrorEscalationFromTransientToCritical() throws Exception {
        // Setup
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        Exception transientError = new RuntimeException("ThrottlingException: Rate exceeded");
        Exception criticalError = new RuntimeException("DisabledException: Key is disabled");

        // Act: Start with transient, then escalate to critical
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, transientError, FailureType.TRANSIENT);

        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        
        // Verify transient state initially
        assertEquals("Failure type should be TRANSIENT", FailureType.TRANSIENT, state.failureType);
        assertFalse("Blocks should not be applied for transient error", state.blocksApplied);

        // Escalate to critical
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, criticalError, FailureType.CRITICAL);

        // Assert: Verify state escalation and blocks applied
        assertTrue("Blocks should be applied after escalation", state.blocksApplied);
        assertEquals("Failure type should be CRITICAL", FailureType.CRITICAL, state.failureType);
    }

    /**
     * Success removes blocks and clears failure tracker
     */
    public void testSuccessRemovesBlocks() throws Exception {
        // Setup: Simulate existing blocks
        Settings settingsWithBlocks = Settings.builder().put("index.blocks.read", true).put("index.blocks.write", true).build();
        when(mockIndexMetadata.getSettings()).thenReturn(settingsWithBlocks);

        // First report a critical failure to establish state
        Exception criticalError = new RuntimeException("DisabledException: Key is disabled");
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, criticalError, FailureType.CRITICAL);

        // Verify failure is tracked with blocksApplied=true
        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        assertTrue("Failure should be in tracker", tracker.containsKey(TEST_INDEX_UUID));
        assertTrue("Blocks should be applied", tracker.get(TEST_INDEX_UUID).blocksApplied);

        // Act: Report success
        monitor.reportSuccess(TEST_INDEX_UUID, TEST_INDEX_NAME);

        // Assert: Verify failure tracker cleared
        assertFalse("Failure should be cleared from tracker", tracker.containsKey(TEST_INDEX_UUID));
    }

    /**
     * Success when no blocks exist is safe
     */
    public void testSuccessWithoutBlocksNoOp() throws Exception {
        // Setup: No blocks exist
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        // First report a transient failure (no blocks applied)
        Exception transientError = new RuntimeException("ThrottlingException: Rate exceeded");
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, transientError, FailureType.TRANSIENT);

        // Act: Report success
        monitor.reportSuccess(TEST_INDEX_UUID, TEST_INDEX_NAME);

        Thread.sleep(100);

        // Assert: No block operations since no blocks to remove
        verify(mockIndicesAdminClient, never()).updateSettings(any());

        // Verify failure tracker cleared
        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        assertFalse("Failure should be cleared from tracker", tracker.containsKey(TEST_INDEX_UUID));
    }

    /**
     * Failure state updates timestamp on subsequent failures
     */
    public void testFailureStateUpdatesTimestamp() throws Exception {
        // Setup
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        Exception error1 = new RuntimeException("Error 1");
        Exception error2 = new RuntimeException("Error 2");

        // Act: Report first failure
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, error1, FailureType.TRANSIENT);

        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        long firstTimestamp = state.lastFailureTimeMillis.get();

        // Wait a bit to ensure timestamp difference
        Thread.sleep(50);

        // Report second failure
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, error2, FailureType.TRANSIENT);

        // Assert: Timestamp updated
        long secondTimestamp = state.lastFailureTimeMillis.get();
        assertTrue("Timestamp should be updated", secondTimestamp > firstTimestamp);
        assertEquals("Exception should be updated", error2, state.lastException.get());
    }

    /**
     * Failure type can upgrade from TRANSIENT to CRITICAL but not downgrade
     */
    public void testFailureTypeUpgradeOnly() throws Exception {
        // Setup
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        Exception transientError = new RuntimeException("ThrottlingException");
        Exception criticalError = new RuntimeException("DisabledException");

        // Act: Start with transient
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, transientError, FailureType.TRANSIENT);

        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        assertEquals(FailureType.TRANSIENT, state.failureType);

        // Upgrade to critical
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, criticalError, FailureType.CRITICAL);
        assertEquals("Should upgrade to CRITICAL", FailureType.CRITICAL, state.failureType);

        // Try to "downgrade" back to transient
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, transientError, FailureType.TRANSIENT);
        assertEquals("Should remain CRITICAL", FailureType.CRITICAL, state.failureType);
    }

    /**
     * Blocks applied only on first critical failure
     */
    public void testBlocksAppliedOnlyOnce() throws Exception {
        // Setup
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        Exception criticalError = new RuntimeException("DisabledException: Key is disabled");

        // Act: Report multiple critical failures
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, criticalError, FailureType.CRITICAL);
        
        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        
        // Verify blocksApplied is true after first failure
        assertTrue("Blocks should be applied after first critical failure", state.blocksApplied);

        // Report additional failures
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, criticalError, FailureType.CRITICAL);
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, criticalError, FailureType.CRITICAL);

        // Assert: blocksApplied flag remains true (not reset or changed)
        assertTrue("Blocks should remain applied", state.blocksApplied);
        assertEquals("Failure type should remain CRITICAL", FailureType.CRITICAL, state.failureType);
    }

    /**
     * Multiple indices can have independent failure states
     * 
     * This test verifies that the failure tracker maintains independent state for different indices.
     */
    public void testMultipleIndicesIndependentState() throws Exception {
        // Setup
        String index1Uuid = "index1-uuid";
        String index1Name = "index1";
        String index2Uuid = "index2-uuid";
        String index2Name = "index2";

        // Mock metadata for both indices
        IndexMetadata mockIndex1Metadata = mock(IndexMetadata.class);
        IndexMetadata mockIndex2Metadata = mock(IndexMetadata.class);
        when(mockIndex1Metadata.getSettings()).thenReturn(Settings.EMPTY);
        when(mockIndex2Metadata.getSettings()).thenReturn(Settings.EMPTY);
        when(mockMetadata.index(index1Name)).thenReturn(mockIndex1Metadata);
        when(mockMetadata.index(index2Name)).thenReturn(mockIndex2Metadata);

        Exception transientError = new RuntimeException("ThrottlingException");
        Exception criticalError = new RuntimeException("DisabledException");

        // Act: Different failures for different indices
        monitor.reportFailure(index1Uuid, index1Name, transientError, FailureType.TRANSIENT);
        monitor.reportFailure(index2Uuid, index2Name, criticalError, FailureType.CRITICAL);

        // Assert: Independent states (synchronous verification only)
        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();

        FailureState state1 = tracker.get(index1Uuid);
        assertNotNull("Index1 should be tracked", state1);
        assertEquals("Index1 failure type should be TRANSIENT", FailureType.TRANSIENT, state1.failureType);
        assertFalse("Index1 should not have blocksApplied flag set", state1.blocksApplied);

        FailureState state2 = tracker.get(index2Uuid);
        assertNotNull("Index2 should be tracked", state2);
        assertEquals("Index2 failure type should be CRITICAL", FailureType.CRITICAL, state2.failureType);
        assertTrue("Index2 should have blocksApplied flag set", state2.blocksApplied);
    }

    /**
     * Null index name is handled gracefully
     */
    public void testNullIndexNameHandledGracefully() throws Exception {
        Exception error = new RuntimeException("Test error");

        // Should not throw exception
        monitor.reportFailure(TEST_INDEX_UUID, null, error, FailureType.CRITICAL);

        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        assertNotNull("Failure should still be tracked", state);
        assertFalse("Blocks should not be applied with null index name", state.blocksApplied);
    }

    /**
     * Success for non-existent failure is safe
     */
    public void testSuccessForNonExistentFailure() throws Exception {
        // Act: Report success for index that never failed
        monitor.reportSuccess("non-existent-uuid", "non-existent-index");

        // Assert: Should not throw exception, tracker remains empty
        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        assertFalse(tracker.containsKey("non-existent-uuid"));
    }

    /**
     * Test unknown errors default to TRANSIENT 
     * This validates the safer default behavior where unknown errors don't block indices
     */
    public void testUnknownErrorsDefaultToTransient() throws Exception {
        // Setup
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        // Unknown/unexpected error that doesn't match any critical or transient pattern
        Exception unknownError = new RuntimeException("Some unexpected random error XYZ-999");

        // Classify the error (should default to TRANSIENT)
        FailureType type = KeyCacheException.classify(unknownError);
        assertEquals("Unknown errors should default to TRANSIENT", FailureType.TRANSIENT, type);

        // Act: Report this unknown error
        monitor.reportFailure(TEST_INDEX_UUID, TEST_INDEX_NAME, unknownError, type);

        Thread.sleep(100);

        // Assert: No blocks applied (transient errors don't trigger blocks)
        verify(mockIndicesAdminClient, never()).updateSettings(any());

        ConcurrentHashMap<String, FailureState> tracker = getFailureTracker();
        FailureState state = tracker.get(TEST_INDEX_UUID);
        assertNotNull("Failure should be tracked", state);
        assertFalse("Blocks should not be applied for unknown error", state.blocksApplied);
        assertEquals("Failure type should be TRANSIENT", FailureType.TRANSIENT, state.failureType);
    }

    /**
     * Test explicit critical error patterns are correctly identified
     */
    public void testExplicitCriticalErrorPatterns() throws Exception {
        // Setup
        when(mockIndexMetadata.getSettings()).thenReturn(Settings.EMPTY);

        // Test various critical error patterns
        String[] criticalPatterns = {
            "DisabledException: Key is disabled",
            "AccessDeniedException: Access denied",
            "NotFoundException: Key not found",
            "access denied to resource"
        };

        for (String pattern : criticalPatterns) {
            Exception criticalError = new RuntimeException(pattern);
            FailureType type = KeyCacheException.classify(criticalError);
            assertEquals("Pattern '" + pattern + "' should be classified as CRITICAL", 
                FailureType.CRITICAL, type);
        }
    }

    /**
     * Test explicit transient error patterns are correctly identified
     */
    public void testExplicitTransientErrorPatterns() throws Exception {
        // Test various transient error patterns
        String[] transientPatterns = {
            "ThrottlingException: Rate exceeded",
            "RequestLimitExceeded",
            "503 Service Unavailable",
            "Connection timeout occurred",
            "Network error: Failed to connect" };

        for (String pattern : transientPatterns) {
            Exception transientError = new RuntimeException(pattern);
            FailureType type = KeyCacheException.classify(transientError);
            assertEquals("Pattern '" + pattern + "' should be classified as TRANSIENT", FailureType.TRANSIENT, type);
        }
    }

    /**
     * Test AWS SDK exception types detected via class name (most reliable method)
     * This simulates what happens when DisabledException is wrapped in KeyCacheException
     */
    public void testExceptionTypeDetectionForWrappedAwsSdkExceptions() throws Exception {
        // Create a real DisabledException class for testing
        class DisabledException extends Exception {
            DisabledException(String message) {
                super(message);
            }
        }

        Exception awsKmsDisabled = new DisabledException("arn:aws:kms:us-east-1:...:key/... is disabled.");

        Exception wrapped = new KeyCacheException("Failed to load key for index: logs-221998", awsKmsDisabled);

        // The class name check should detect "DisabledException" in the cause chain
        // even though the message doesn't contain "DisabledException"
        FailureType type = KeyCacheException.classify(wrapped);

        assertEquals("Wrapped DisabledException should be classified as CRITICAL", FailureType.CRITICAL, type);
    }
}

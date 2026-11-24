/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.cipher;

import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;

import javax.crypto.Cipher;
import javax.crypto.Mac;

import org.opensearch.index.store.key.HkdfKeyDerivation;
import org.opensearch.index.translog.TranslogChunkManager;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Asserts leftover JCE Cipher / Mac / MessageDigest sites report the Provider passed in.
 */
public class CryptoProviderWiringTests extends OpenSearchTestCase {

    private static final Provider SUN_JCE = Security.getProvider("SunJCE");

    public void testPooledCtrCipherUsesPassedProvider() {
        Cipher cipher = AesCipherFactory.getPooledCipher(SUN_JCE);
        assertEquals("SunJCE", cipher.getProvider().getName());
    }

    public void testGcmCipherUsesPassedProvider() {
        Cipher cipher = AesGcmCipherFactory.getCipher(SUN_JCE);
        assertEquals("SunJCE", cipher.getProvider().getName());
    }

    public void testHkdfMacUsesPassedProvider() throws Exception {
        Mac mac = HkdfKeyDerivation.getMac(SUN_JCE);
        assertEquals("SunJCE", mac.getProvider().getName());
    }

    public void testTranslogCipherUsesPassedProvider() {
        Cipher cipher = TranslogChunkManager.getPooledCipher(SUN_JCE);
        assertEquals("SunJCE", cipher.getProvider().getName());
    }

    public void testTranslogDigestUsesPassedProvider() throws Exception {
        MessageDigest digest = TranslogChunkManager.getMessageDigest(SUN_JCE);
        if (SUN_JCE.getService("MessageDigest", "SHA-256") != null) {
            assertEquals("SunJCE", digest.getProvider().getName());
        } else {
            // SunJCE has no SHA-256; fallback is JCA's default search (same as the old no-arg call).
            MessageDigest jcaDefault = MessageDigest.getInstance("SHA-256");
            assertEquals(jcaDefault.getProvider().getName(), digest.getProvider().getName());
        }
    }
}

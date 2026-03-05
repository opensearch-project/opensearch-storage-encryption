/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.metrics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileOpenTracker {
    private static final Logger LOGGER = LogManager.getLogger(FileOpenTracker.class);
    private static final Map<String, AtomicLong> OPENS = new ConcurrentHashMap<>();
    private static final String OUTPUT_FILE = System.getProperty("user.home") + "/file_open_stats.csv";

    public static void trackOpen(String path) {
        OPENS.computeIfAbsent(path, k -> new AtomicLong()).incrementAndGet();
    }

    public static void logStats() {
        LOGGER.info("=== File Open Stats ===");
        LOGGER.info("Total unique files: {}", OPENS.size());
        LOGGER.info("Total opens: {}", OPENS.values().stream().mapToLong(AtomicLong::get).sum());
        writeToFile();
    }

    @SuppressWarnings("removal")
    public static void writeToFile() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                long totalOpens = OPENS.values().stream().mapToLong(AtomicLong::get).sum();

                StringBuilder sb = new StringBuilder();
                sb.append("\n=== ").append(timestamp).append(" ===\n");
                sb.append("Total unique files: ").append(OPENS.size()).append("\n");
                sb.append("Total opens: ").append(totalOpens).append("\n");
                sb.append("path,count\n");
                OPENS
                    .entrySet()
                    .stream()
                    .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                    .forEach(e -> sb.append(e.getKey()).append(",").append(e.getValue().get()).append("\n"));

                Files.writeString(Paths.get(OUTPUT_FILE), sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                LOGGER.info("File open stats appended to: {}", OUTPUT_FILE);
            } catch (IOException e) {
                LOGGER.warn("Failed to write file open stats to file", e);
            }
            return null;
        });
    }

    public static void reset() {
        OPENS.clear();
    }
}

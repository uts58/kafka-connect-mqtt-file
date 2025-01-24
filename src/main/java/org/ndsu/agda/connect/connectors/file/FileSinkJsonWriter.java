package org.ndsu.agda.connect.connectors.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileSinkJsonWriter {
    private final Logger log = LoggerFactory.getLogger(FileSinkJsonWriter.class);
    private static final long WRITER_TIMEOUT_MILLIS = 5 * 60 * 1000; // 5 minutes
    private final Map<String, WriterEntry> writers = new ConcurrentHashMap<>();

    private static class WriterEntry {
        final BufferedWriter writer;
        Instant lastAccessTime;

        WriterEntry(BufferedWriter writer) {
            this.writer = writer;
            this.lastAccessTime = Instant.now();
        }

        void updateAccessTime() {
            this.lastAccessTime = Instant.now();
        }
    }

    public void write(Path filePath, String json) throws IOException {
        try {
            writeJsonToFile(filePath, json);
        } catch (FileNotFoundException e) {
            log.warn("Folder not found: {}, Creating folder", filePath.getParent());
            Files.createDirectories(filePath.getParent());
            writeJsonToFile(filePath, json);
        }
    }

    public void writeJsonToFile(Path filePath, String json) throws IOException {
        String filePathStr = filePath.toString();
        WriterEntry entry;
        synchronized (writers) {
            entry = writers.get(filePathStr);
            if (entry == null) {
                BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.toFile(), true));
                entry = new WriterEntry(writer);
                writers.put(filePathStr, entry);
                log.info("Created new writer for {}, current number of writers {}", filePathStr, writers.size());
            }
            entry.updateAccessTime();
        }

        synchronized (entry.writer) {
            entry.writer.write(json);
            entry.writer.newLine();
            entry.writer.flush();
        }

        log.info("Successfully wrote record to {}", filePath);
    }

    public void cleanUpWriters() {
        synchronized (writers) {
            Iterator<Map.Entry<String, WriterEntry>> iterator = writers.entrySet().iterator();
            Instant now = Instant.now();

            while (iterator.hasNext()) {
                Map.Entry<String, WriterEntry> entry = iterator.next();
                WriterEntry writerEntry = entry.getValue();


                if (now.toEpochMilli() - writerEntry.lastAccessTime.toEpochMilli() > WRITER_TIMEOUT_MILLIS) {
                    try {
                        writerEntry.writer.close();
                        log.info("Closed inactive writer for file: {}", entry.getKey());
                    } catch (IOException e) {
                        log.error("Failed to close inactive writer for file: {}", entry.getKey(), e);
                    }
                    iterator.remove();
                }
            }
        }
    }

    public void closeWriters() {
        synchronized (writers) {
            for (WriterEntry entry : writers.values()) {
                try {
                    entry.writer.close();
                } catch (IOException e) {
                    log.error("Failed to close writer during shutdown", e);
                }
            }
            writers.clear();
        }
    }

    public void startWriterCleanupScheduler() {
        java.util.concurrent.ScheduledExecutorService scheduler =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::cleanUpWriters, 5, 5, java.util.concurrent.TimeUnit.MINUTES);
    }
}

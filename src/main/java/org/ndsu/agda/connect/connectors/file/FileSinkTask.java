package org.ndsu.agda.connect.connectors.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.ndsu.agda.connect.Version;
import org.ndsu.agda.connect.config.FileSInkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;

public class FileSinkTask extends SinkTask {
    private final Logger log = LoggerFactory.getLogger(FileSinkTask.class);
    private String storageDirectory;
    private ObjectMapper objectMapper;
    FileSinkJsonWriter writer;


    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.storageDirectory = new FileSInkConnectorConfig(map)
                .getString(FileSInkConnectorConfig.STORAGE_DIRECTORY);
        this.objectMapper = new ObjectMapper();
        this.writer = new FileSinkJsonWriter();
        writer.startWriterCleanupScheduler();

        try {
            Path storagePath = Paths.get(storageDirectory);
            if (Files.exists(storagePath)) {
                log.info("Storage directory exists: {}", storageDirectory);
            } else {
                log.warn("Storage directory does not exist. Creating: {}", storageDirectory);
                Files.createDirectories(storagePath);
                log.info("Storage directory created: {}", storageDirectory);
            }
        } catch (IOException e) {
            log.error("Failed to create or access storage directory: {}", storageDirectory, e);
            throw new ConnectException("Failed to initialize storage directory", e);
        }

        log.info("FileSinkTask started with storage directory: {}", storageDirectory);
    }


    public String datePathExtractor(Object timestamp) {
        ZonedDateTime zdt = ZonedDateTime.parse((String) timestamp);
        return zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        records.forEach(record -> {
            try {
                Map<String, Object> payload = objectMapper.readValue(record.value().toString(), Map.class);
                Map<String, Object> iotNode = (Map<String, Object>) payload.get("iotnode");

                Path filePath = Paths.get(
                        storageDirectory,
                        (String) iotNode.get("_id")
                ).resolve(datePathExtractor(iotNode.get("updatedAt")) + ".jsonl");

                try {
                    writer.writeJsonToFile(filePath, objectMapper.writeValueAsString(payload));
                } catch (FileNotFoundException e) {
                    log.warn("Folder not found: {}, Creating folder", filePath.getParent());
                    Files.createDirectories(filePath.getParent());
                    writer.writeJsonToFile(filePath, objectMapper.writeValueAsString(payload));
                }
            } catch (Exception e) {
                log.error("Failed to process record: {}", record.value(), e);
            }
        });
    }

    @Override
    public void stop() {
        writer.closeWriters();
    }
}

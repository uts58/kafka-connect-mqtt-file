package org.ndsu.agda.connect.connectors.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.ndsu.agda.connect.Version;
import org.ndsu.agda.connect.config.FileSInkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;

public class FileSinkTask extends SinkTask {
    private Logger log = LoggerFactory.getLogger(FileSinkTask.class);
    private FileSInkConnectorConfig config;
    private String storageDirectory;
    private ObjectMapper objectMapper;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        config = new FileSInkConnectorConfig(map);
        this.storageDirectory = config.getString(FileSInkConnectorConfig.STORAGE_DIRECTORY);
        this.objectMapper = new ObjectMapper();

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

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            try {
                // Parse the record value into a Map
                Map<String, Object> payload = objectMapper.readValue(record.value().toString(), Map.class);

                // Safely extract the "iotnode" object and cast it to a Map
                Object iotnodeObj = payload.get("iotnode");
                if (iotnodeObj instanceof Map) {
                    Map<String, Object> iotnode = (Map<String, Object>) iotnodeObj;

                    // Safely extract "_id"
                    String id = (String) iotnode.get("_id");
                    if (id == null || id.isEmpty()) {
                        log.warn("Record missing '_id' field in iotnode: {}", iotnode);
                        continue;
                    }

                    // Create a folder using the _id
                    Path folderPath = Paths.get(storageDirectory, id);
                    Files.createDirectories(folderPath);

                    // Write data to a JSONL file
                    Path filePath = folderPath.resolve("data.jsonl");
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.toFile(), true))) {
                        String jsonLine = objectMapper.writeValueAsString(payload);
                        writer.write(jsonLine);
                        writer.newLine();
                        writer.flush(); // Ensure data is written immediately
                    }

                    log.info("Successfully wrote record with _id: {} to {}", id, filePath);

                } else {
                    log.warn("'iotnode' field is either missing or not a valid Map in payload: {}", payload);
                }

            } catch (Exception e) {
                log.error("Failed to process record: {}", record.value(), e);
            }
        }
    }


    @Override
    public void stop() {

    }
}

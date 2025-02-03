package org.ndsu.agda.connect.connectors.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.ndsu.agda.connect.Version;
import org.ndsu.agda.connect.config.FileSInkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private Path generalFailedDataFilePath;
    private Path jsonFailedDataFilePath;


    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.storageDirectory = new FileSInkConnectorConfig(map).getString(FileSInkConnectorConfig.STORAGE_DIRECTORY);

        String failedDataDirName = "failedData";
        this.generalFailedDataFilePath = Paths.get(storageDirectory, failedDataDirName).resolve("general_failure.txt");
        this.jsonFailedDataFilePath = Paths.get(storageDirectory, failedDataDirName).resolve("json_parsing_error.txt");

        this.objectMapper = new ObjectMapper();
        this.writer = new FileSinkJsonWriter();
        writer.startWriterCleanupScheduler();

        try {
            Path storagePath = Paths.get(storageDirectory, failedDataDirName);
            Files.createDirectories(storagePath);
            log.info("Storage directory ready: {}", storagePath);
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
                @SuppressWarnings("unchecked")
                Map<String, Object> payload = (Map<String, Object>) record.value();
                @SuppressWarnings("unchecked")
                Map<String, Object> iotNode = (Map<String, Object>) payload.get("iotnode");

                if (iotNode == null || iotNode.get("_id") == null) {
                    throw new IllegalArgumentException("Missing required field '_id' or 'iotnode'");
                }

                Path filePath = Paths.get(
                        storageDirectory,
                        (String) iotNode.get("_id")
                ).resolve(datePathExtractor(iotNode.get("reportedAt")) + ".jsonl");

                String json = objectMapper.writeValueAsString(payload);

                writer.write(filePath, json);

            } catch (ClassCastException | IllegalArgumentException e) {
                log.error("Failed to parse or extract data from JSON: {}", record.value(), e);
                writer.write(jsonFailedDataFilePath, record.value().toString());
            } catch (Exception e) {
                log.error("Unexpected error processing record: {}", record.value(), e);
                writer.write(generalFailedDataFilePath, record.value().toString());
            }
        });
    }

    @Override
    public void stop() {
        writer.closeWriters();
    }
}

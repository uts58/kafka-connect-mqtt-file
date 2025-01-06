package org.ndsu.agda.connect.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FileSInkConnectorConfig extends AbstractConfig {
    public static final String STORAGE_DIRECTORY = "storage.directory";
    public static final String STORAGE_DIRECTORY_DOC = "Directory to store the files in";
    public static final String KAFKA_TOPIC = "topics";
    public static final String KAFKA_TOPIC_DOC = "List of kafka topics to consume from";

    public FileSInkConnectorConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(STORAGE_DIRECTORY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, STORAGE_DIRECTORY_DOC)
                .define(KAFKA_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, KAFKA_TOPIC_DOC);
    }
}
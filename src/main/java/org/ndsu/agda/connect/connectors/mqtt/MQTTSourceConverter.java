package org.ndsu.agda.connect.connectors.mqtt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.ndsu.agda.connect.config.MQTTSourceConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts a MQTT message to a Kafka message
 */
public class MQTTSourceConverter {

    private MQTTSourceConnectorConfig mqttSourceConnectorConfig;

    private final Logger log = LoggerFactory.getLogger(MQTTSourceConverter.class);

    public MQTTSourceConverter(MQTTSourceConnectorConfig mqttSourceConnectorConfig) {
        this.mqttSourceConnectorConfig = mqttSourceConnectorConfig;
    }

    protected SourceRecord convert(String topic, MqttMessage mqttMessage) {
        log.debug("Converting MQTT message: {}", mqttMessage);

        String payload = new String(mqttMessage.getPayload());

        // Create an ObjectMapper instance (you might want to reuse a singleton instance in production)
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap;

        try {
            jsonMap = mapper.readValue(payload, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            log.error("Failed to parse JSON payload, passing raw payload in a wrapper", e);
            // In case of parsing error, you can choose to wrap the raw payload in a Map
            jsonMap = Collections.singletonMap("raw", payload);
        }

        SourceRecord sourceRecord = new SourceRecord(
                new HashMap<>(),  // source partition
                new HashMap<>(),  // source offset
                this.mqttSourceConnectorConfig.getString(MQTTSourceConnectorConfig.KAFKA_TOPIC),
                null,           // null schema: using schemaless mode
                jsonMap         // structured JSON object as value
        );

        log.debug("Converted MQTT Message: {}", sourceRecord);
        return sourceRecord;
    }
}

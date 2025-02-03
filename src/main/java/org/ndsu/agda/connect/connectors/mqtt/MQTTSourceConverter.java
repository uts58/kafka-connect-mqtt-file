package org.ndsu.agda.connect.connectors.mqtt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.ndsu.agda.connect.config.MQTTSourceConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts a MQTT message to a Kafka message
 */
public class MQTTSourceConverter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Logger log = LoggerFactory.getLogger(MQTTSourceConverter.class);
    private final String KAFKA_TOPIC;

    public MQTTSourceConverter(MQTTSourceConnectorConfig mqttSourceConnectorConfig) {
        this.KAFKA_TOPIC = mqttSourceConnectorConfig.getString(MQTTSourceConnectorConfig.KAFKA_TOPIC);
    }

    protected SourceRecord convert(String topic, MqttMessage mqttMessage) {
        log.debug("Converting MQTT message: {}", mqttMessage);

        String payload = new String(mqttMessage.getPayload(), java.nio.charset.StandardCharsets.UTF_8);
        Map<String, Object> jsonMap;

        try {
            jsonMap = OBJECT_MAPPER.readValue(payload, new TypeReference<Map<String, Object>>() {
            });
            jsonMap.put("mqttReceivedAt", Instant.now().toString());

            if (jsonMap.containsKey("iotnode")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> iotNode = (Map<String, Object>) jsonMap.get("iotnode");
                if (iotNode.containsKey("reportedAt")) {
                    jsonMap.put("baseReportedAt", iotNode.get("reportedAt").toString());
                }
            }

        } catch (IOException e) {
            log.error("Failed to parse JSON payload, passing raw payload in a wrapper", e);
            jsonMap = Collections.singletonMap("raw", payload);
        }

        SourceRecord sourceRecord = new SourceRecord(
                new HashMap<>(),  // source partition
                new HashMap<>(),  // source offset
                this.KAFKA_TOPIC,
                null,           // null schema: using schemaless mode
                jsonMap         // structured JSON object as value
        );

        log.debug("Converted MQTT Message: {}", sourceRecord);
        return sourceRecord;
    }
}

package be.jovacon.kafka.connect;

import be.jovacon.kafka.connect.config.MQTTSourceConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Converts a MQTT message to a Kafka message
 */
public class MQTTSourceConverter {

    private MQTTSourceConnectorConfig mqttSourceConnectorConfig;

    private Logger log = LoggerFactory.getLogger(MQTTSourceConverter.class);

    public MQTTSourceConverter(MQTTSourceConnectorConfig mqttSourceConnectorConfig) {
        this.mqttSourceConnectorConfig = mqttSourceConnectorConfig;
    }

    protected SourceRecord convert(String topic, MqttMessage mqttMessage) {
        log.debug("Converting MQTT message: " + mqttMessage);

        SourceRecord sourceRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                this.mqttSourceConnectorConfig.getString(MQTTSourceConnectorConfig.KAFKA_TOPIC),
                Schema.STRING_SCHEMA,
                new String(mqttMessage.getPayload())
        );

        log.debug("Converted MQTT Message: " + sourceRecord);
        return sourceRecord;
    }
}

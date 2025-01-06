package org.ndsu.agda.connect.connectors.mqtt;

import org.ndsu.agda.connect.config.MQTTSinkConnectorConfig;
import org.ndsu.agda.connect.config.MQTTSourceConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a Kafka message to a MQTT Message
 */
public class MQTTSinkConverter {

    private MQTTSinkConnectorConfig mqttSinkConnectorConfig;

    private Logger log = LoggerFactory.getLogger(MQTTSinkConverter.class);

    public MQTTSinkConverter(MQTTSinkConnectorConfig mqttSinkConnectorConfig) {
        this.mqttSinkConnectorConfig = mqttSinkConnectorConfig;
    }

    protected MqttMessage convert(SinkRecord sinkRecord) {
        log.trace("Converting Kafka message");

        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(((String)sinkRecord.value()).getBytes());
        mqttMessage.setQos(this.mqttSinkConnectorConfig.getInt(MQTTSourceConnectorConfig.MQTT_QOS));
        log.trace("Result MQTTMessage: " + mqttMessage);
        return mqttMessage;
    }
}

package org.ndsu.agda.connect.connectors.mqtt;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.paho.mqttv5.client.IMqttClient;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.ndsu.agda.connect.Version;
import org.ndsu.agda.connect.config.MQTTSinkConnectorConfig;
import org.ndsu.agda.connect.config.MQTTSourceConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of the Kafka Connect Sink task
 */
public class MQTTSinkTask extends SinkTask {

    private Logger log = LoggerFactory.getLogger(MQTTSinkTask.class);
    private MQTTSinkConnectorConfig config;
    private MQTTSinkConverter mqttSinkConverter;

    private IMqttClient mqttClient;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        config = new MQTTSinkConnectorConfig(map);
        mqttSinkConverter = new MQTTSinkConverter(config);
        try {
            mqttClient = new MqttClient(
                    config.getString(MQTTSinkConnectorConfig.BROKER),
                    config.getString(MQTTSinkConnectorConfig.CLIENTID),
                    new MemoryPersistence()
            );

            log.info("Connecting to MQTT Broker " + config.getString(MQTTSourceConnectorConfig.BROKER));
            connect(mqttClient);
            log.info("Connected to MQTT Broker. This connector publishes to the " + this.config.getString(MQTTSinkConnectorConfig.MQTT_TOPIC) + " topic");

        } catch (MqttException e) {
            throw new ConnectException(e);
        }
    }

    private void connect(IMqttClient mqttClient) throws MqttException {
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(config.getBoolean(MQTTSinkConnectorConfig.MQTT_CLEANSESSION));
        connOpts.setKeepAliveInterval(config.getInt(MQTTSinkConnectorConfig.MQTT_KEEPALIVEINTERVAL));
        connOpts.setConnectionTimeout(config.getInt(MQTTSinkConnectorConfig.MQTT_CONNECTIONTIMEOUT));
        connOpts.setAutomaticReconnect(config.getBoolean(MQTTSinkConnectorConfig.MQTT_ARC));

        if (!config.getString(MQTTSourceConnectorConfig.MQTT_USERNAME).isEmpty()
                && config.getPassword(MQTTSourceConnectorConfig.MQTT_PASSWORD) != null
                && !config.getPassword(MQTTSourceConnectorConfig.MQTT_PASSWORD).value().isEmpty()) {
            connOpts.setUserName(config.getString(MQTTSourceConnectorConfig.MQTT_USERNAME));
            connOpts.setPassword(
                    config.getPassword(MQTTSourceConnectorConfig.MQTT_PASSWORD).value().getBytes(StandardCharsets.UTF_8)
            );
        }

        log.debug("MQTT Connection properties: " + connOpts);

        mqttClient.connect(connOpts);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        try {
            for (Iterator<SinkRecord> iterator = collection.iterator(); iterator.hasNext(); ) {
                SinkRecord sinkRecord = iterator.next();
                log.debug("Received message with offset " + sinkRecord.kafkaOffset());
                MqttMessage mqttMessage = mqttSinkConverter.convert(sinkRecord);
                if (!mqttClient.isConnected()) mqttClient.connect();
                log.debug("Publishing message to topic " + this.config.getString(MQTTSinkConnectorConfig.MQTT_TOPIC) + " with payload " + new String(mqttMessage.getPayload()));
                mqttClient.publish(this.config.getString(MQTTSinkConnectorConfig.MQTT_TOPIC), mqttMessage);
            }
        } catch (MqttException e) {
            throw new ConnectException(e);
        }

    }

    @Override
    public void stop() {
        if (mqttClient.isConnected()) {
            try {
                log.debug("Disconnecting from MQTT Broker " + config.getString(MQTTSinkConnectorConfig.BROKER));
                mqttClient.disconnect();
            } catch (MqttException mqttException) {
                log.error("Exception thrown while disconnecting client.", mqttException);
            }
        }
    }
}

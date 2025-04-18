package org.ndsu.agda.connect.connectors.mqtt;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.ndsu.agda.connect.Version;
import org.ndsu.agda.connect.config.MQTTSourceConnectorConfig;
import org.ndsu.agda.connect.utils.SourceRecordDeque;
import org.ndsu.agda.connect.utils.SourceRecordDequeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Actual implementation of the Kafka Connect MQTT Source Task
 */
public class MQTTSourceTask extends SourceTask implements IMqttMessageListener {

    private final Logger log = LoggerFactory.getLogger(MQTTSourceTask.class);
    private MQTTSourceConnectorConfig config;
    private MQTTSourceConverter mqttSourceConverter;
    private SourceRecordDeque sourceRecordDeque;

    private IMqttClient mqttClient;

    public void start(Map<String, String> props) {
        config = new MQTTSourceConnectorConfig(props);
        mqttSourceConverter = new MQTTSourceConverter(config);
        this.sourceRecordDeque = SourceRecordDequeBuilder.of()
                .batchSize(4096).emptyWaitMs(100).
                maximumCapacityTimeoutMs(60000).maximumCapacity(50000).build();
        try {
            String clientId = config.getString(MQTTSourceConnectorConfig.CLIENTID);
            log.info("Connecting with clientID={}", clientId);
            mqttClient = new MqttClient(config.getString(MQTTSourceConnectorConfig.BROKER), clientId, new MemoryPersistence());

            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void disconnected(MqttDisconnectResponse disconnectResponse) {
                    log.error("MQTT Connection Lost {}", disconnectResponse);
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    onMessageReceived("messageArrivedCallback", topic, message);
                }

                @Override
                public void deliveryComplete(IMqttToken token) {
                    log.info("MQTT Delivery Complete{}", token);
                }

                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    log.info("MQTT Connection Complete");
                    subscribe(mqttClient);
                }

                @Override
                public void authPacketArrived(int reasonCode, MqttProperties properties) {

                }

                @Override
                public void mqttErrorOccurred(MqttException exception) {
                    log.error("An MQTT error occurred", exception);
                }
            });

            connect(mqttClient);
        } catch (MqttException e) {
            throw new ConnectException(e);
        }
    }

    public void onMessageReceived(String source, String topic, MqttMessage message) {
        try {
            SourceRecord record = mqttSourceConverter.convert(topic, message);
            log.info("Message arrived from topic {} from source {}", topic, source);
            sourceRecordDeque.add(record);
        } catch (Exception e) {
            log.error("Error on message received ", e);
        }
    }

    private void subscribe(IMqttClient mqttClient) {
        String topicSubscription = this.config.getString(MQTTSourceConnectorConfig.MQTT_TOPIC);
        try {
            int qosLevel = this.config.getInt(MQTTSourceConnectorConfig.MQTT_QOS);

            log.info("Subscribing to {} with QOS {}", topicSubscription, qosLevel);
            //freaking callback function doesn't work here for mqttv5... I spent 3 days to figure it out, guess I'm dumb :|
            mqttClient.subscribe(topicSubscription, qosLevel);
            log.info("Subscribed to {} with QOS {}", topicSubscription, qosLevel);
        } catch (Exception e) {
            log.error("Error subscribing to topic {}", topicSubscription, e);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {

            }
        }
    }

    private void connect(IMqttClient mqttClient) {
        try {
            log.info("Connecting to MQTT Broker {}", config.getString(MQTTSourceConnectorConfig.BROKER));
            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(config.getBoolean(MQTTSourceConnectorConfig.MQTT_CLEANSESSION));
            connOpts.setKeepAliveInterval(config.getInt(MQTTSourceConnectorConfig.MQTT_KEEPALIVEINTERVAL));
            connOpts.setConnectionTimeout(config.getInt(MQTTSourceConnectorConfig.MQTT_CONNECTIONTIMEOUT));
            connOpts.setAutomaticReconnect(config.getBoolean(MQTTSourceConnectorConfig.MQTT_ARC));

            if (!config.getString(MQTTSourceConnectorConfig.MQTT_USERNAME).isEmpty() && config.getPassword(MQTTSourceConnectorConfig.MQTT_PASSWORD) != null && !config.getPassword(MQTTSourceConnectorConfig.MQTT_PASSWORD).value().isEmpty()) {
                connOpts.setUserName(config.getString(MQTTSourceConnectorConfig.MQTT_USERNAME));
                connOpts.setPassword(config.getPassword(MQTTSourceConnectorConfig.MQTT_PASSWORD).value().getBytes(StandardCharsets.UTF_8));
            }

            log.info("MQTT Connection properties: {}", connOpts);
            mqttClient.connect(connOpts);
            log.info("Connected to MQTT Broker {}", config.getString(MQTTSourceConnectorConfig.BROKER));
        } catch (Exception e) {
            log.error("Error establishing connection", e);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
    }

    /**
     * method is called periodically by the Connect framework
     *
     * @return
     * @throws InterruptedException
     */
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = sourceRecordDeque.getBatch();
        log.trace("Records returning to poll(): {}", records);
        return records;
    }

    public void stop() {
        if (mqttClient.isConnected()) {
            try {
                log.debug("Disconnecting from MQTT Broker {}", config.getString(MQTTSourceConnectorConfig.BROKER));
                mqttClient.disconnect();
            } catch (MqttException mqttException) {
                log.error("Exception thrown while disconnecting client.", mqttException);
            }
        }
    }

    public String version() {
        return Version.getVersion();
    }

    /**
     * Callback method when a MQTT message arrives at the Topic
     */
    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        log.debug("Message arrived in connector from topic {}", topic);
        SourceRecord record = mqttSourceConverter.convert(topic, mqttMessage);
        log.debug("Converted record: {}", record);
        sourceRecordDeque.add(record);
    }
}

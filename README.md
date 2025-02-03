# kafka-connect-mqtt

This repo contains a MQTT Source and Sink Connector for Apache Kafka. It is tested with Kafka 2+ and Java 21.

Using the Source connector you can subscribe to a MQTT topic and write these messages to a Kafka topic.

The Sink connector works the other way around.

Note: 
* SSL connections are not supported at the moment
* The connector works only with a single task. Settings maxTasks > 1 has no effect.

## Building the connector

To build the connector, you must have the following installed:

* Java 8 or later
* Maven
* GIT

Clone the repository with the following command:
```
git clone https://github.com/uts58/kafka-connect-mqtt-file.git
```
Change directory into the `kafka-connect-mqtt-file` directory
```
cd kafka-connect-mqtt-file
```
Build the connector fat jar using Maven
```
mvn clean package
```
## Installing the connector

### Prerequisites

You must have Kafka 2+ installed


### Installing

* Copy the file `/target/kafka-connect-mqtt-<version>.jar` to your Kafka Connect plugin path
* Restart Kafka Connect
* Check if the connector has been loaded succesfully

```
http://<kafkaconnect>:8083/connector-plugins
```
If you see these entries, the connector has been installed succesfully

```
{
    "class": "org.ndsu.agda.connect.connectors.mqtt.MQTTSourceConnector",
    "type": "source",
    "version": "<version>"
},
{
    "class": "org.ndsu.agda.connect.connectors.mqtt.FileSinkConnector",
    "type": "sink",
    "version": "<version>"
}
```

## Configuring the MQTT Source connector

The MQTT Source connector subscribes to a Topic on a MQTT Broker and sends the messages to a Kafka topic.

Here is a basic configuration example:
```
curl -X POST \
  http://<kafkaconnect>:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{ "name": "mqtt-source-connector",
    "config":
    {
      "connector.class":"org.ndsu.agda.connect.connectors.mqtt.MQTTSourceConnector",
      "mqtt.topic":"my_mqtt_topic",
      "kafka.topic":"my_kafka_topic",
      "mqtt.clientID":"my_client_id",
      "mqtt.broker":"tcp://127.0.0.1:1883",
      "key.converter":"org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable":false,
      "value.converter":"org.apache.kafka.connect.storage.StringConverter",
      "value.converter.schemas.enable":false
    }
}'
```
### Optional Configuration options
* `mqtt.qos` (optional): 0 – At most Once, 1 – At Least Once, 2 – Exactly Once
* `mqtt.automaticReconnect` (optional)(default: true): Should the client automatically reconnect in case of connection failures
* `mqtt.keepAliveInterval` (optional)(default: 60 seconds)
* `mqtt.cleanSession` (optional)(default: true): Controls the state after disconnecting the client from the broker.
* `mqtt.connectionTimeout` (optional)(default: 30 seconds)
* `mqtt.userName` (optional): Username to connect to MQTT broker
* `mqtt.password` (optional): Password to connect to MQTT broker

## Configuring the File Sink connector

The File Sink Connector reads messages from a Kafka topic(s) and dumps them into files.

Here is a basic configuration example:
```
curl -X POST \
  http://<kafkaconnect>>:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{ "name": "mqtt-sink-connector",
    "config":
    {
      "name": "file-sink-connector",
      "config": {
        "connector.class": "org.ndsu.agda.connect.connectors.file.FileSinkConnector",
        "storage.directory": "/home/appuser",
        "topics": "general-topic",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
      }
    }
}'
```

## Authors

* **Johan Vandevenne** - *Initial work* 
* **Utsha Saha** - *Updates*

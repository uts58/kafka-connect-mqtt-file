package org.ndsu.agda.connect.connectors.mqtt;

import org.ndsu.agda.connect.Version;
import org.ndsu.agda.connect.config.MQTTSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Implementation of the Kafka Connect Source connector
 */
public class MQTTSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MQTTSourceConnector.class);
    private Map<String, String> configProps;

    public void start(Map<String, String> map) {
        this.configProps = Collections.unmodifiableMap(map);
    }

    public Class<? extends Task> taskClass() {
        return MQTTSourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.debug("Enter taskconfigs");
        if (maxTasks > 1) {
            log.info("maxTasks is {}. MaxTasks > 1 is not supported in this connector.", maxTasks);
        }
        List<Map<String, String>> taskConfigs = new ArrayList<>(1);
        taskConfigs.add(new HashMap<>(configProps));

        log.debug("Taskconfigs: {}", taskConfigs);
        return taskConfigs;
    }

    public void stop() {

    }

    public ConfigDef config() {
        return MQTTSourceConnectorConfig.configDef();
    }

    public String version() {
        return Version.getVersion();
    }
}

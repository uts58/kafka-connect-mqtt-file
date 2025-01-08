package org.ndsu.agda.connect.connectors.file;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.ndsu.agda.connect.Version;
import org.ndsu.agda.connect.config.FileSInkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FileSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(FileSinkConnector.class);
    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> map) {
        FileSInkConnectorConfig fileSInkConnectorConfig = new FileSInkConnectorConfig(map);
        this.configProps = Collections.unmodifiableMap(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            log.info("maxTasks is {}. MaxTasks > 1 is not supported in this connector.", maxTasks);
        }
        List<Map<String, String>> taskConfigs = new ArrayList<>(1);
        taskConfigs.add(new HashMap<>(configProps));

        log.debug("Taskconfigs: {}", taskConfigs);
        return taskConfigs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return FileSInkConnectorConfig.configDef();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}

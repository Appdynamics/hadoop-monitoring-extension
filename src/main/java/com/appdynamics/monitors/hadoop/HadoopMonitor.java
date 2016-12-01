/**
 * Copyright 2013 AppDynamics
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.appdynamics.monitors.hadoop;

import com.appdynamics.extensions.PathResolver;
import com.appdynamics.extensions.StringUtils;
import com.appdynamics.extensions.conf.MonitorConfiguration;
import com.appdynamics.extensions.conf.MonitorConfiguration.ConfItem;
import com.appdynamics.extensions.util.MetricWriteHelper;
import com.appdynamics.extensions.util.MetricWriteHelperFactory;
import com.appdynamics.monitors.hadoop.communicator.AmbariMetricsFetcherTask;
import com.appdynamics.monitors.hadoop.communicator.ResourceMgrMetricsFetcherTask;
import com.appdynamics.monitors.hadoop.input.MetricConfig;
import com.google.common.base.Strings;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

public class HadoopMonitor extends AManagedMonitor {
    public static final Logger logger = LoggerFactory.getLogger(HadoopMonitor.class);
    public static final String CONFIG_ARG = "config-file";
    public static final String RM_METRICS_XML_ARG = "metrics-resource-manager-file";
    public static final String AMBARI_METRICS_XML_ARG = "metrics-ambari-file";
    private MonitorConfiguration ambariMonitorConfig;
    private MonitorConfiguration resourceMgrMonitorConfig;

    public HadoopMonitor() {
        String msg = "Using Monitor Version [" + getImplementationVersion() + "]";
        logger.info(msg);
        System.out.println(msg);
    }

    public void init(Map<String, String> taskArgs) {
        if (resourceMgrMonitorConfig == null) {
            this.resourceMgrMonitorConfig = initResourceManagerConfig(taskArgs);
        }
        if (ambariMonitorConfig == null) {
            this.ambariMonitorConfig = initAmbariConfig(taskArgs);
        }
    }

    protected MonitorConfiguration initAmbariConfig(Map<String, String> taskArgs) {
        logger.info("Initializing the Ambari Monitor Configuration");
        String configFilename = getConfigFilename(taskArgs.get(CONFIG_ARG));
        MetricWriteHelper metricWriteHelper = MetricWriteHelperFactory.create(this);
        MonitorConfiguration conf = new MonitorConfiguration("Custom Metrics|HadoopMonitor|Ambari",
                new AmbariMonitorExecutor(), metricWriteHelper);
        String xmlPath = getPath(taskArgs, AMBARI_METRICS_XML_ARG, "monitors/HadoopMonitor/metrics-ambari.xml");
        conf.setMetricsXml(xmlPath, MetricConfig.class);
        conf.setConfigYml(configFilename, "ambariMonitor");
        if (conf.isEnabled()) {
            conf.checkIfInitialized(ConfItem.CONFIG_YML, ConfItem.METRIC_PREFIX,
                    ConfItem.METRIC_WRITE_HELPER, ConfItem.METRICS_XML);
        } else {
            logger.info("The Ambari Monitor is not enabled");
        }
        return conf;
    }

    protected MonitorConfiguration initResourceManagerConfig(Map<String, String> taskArgs) {
        logger.info("Initializing the Resource Manager Monitor Configuration");
        String configFilename = getConfigFilename(taskArgs.get(CONFIG_ARG));
        MetricWriteHelper metricWriteHelper = MetricWriteHelperFactory.create(this);
        MonitorConfiguration conf = new MonitorConfiguration("Custom Metrics|HadoopMonitor|ResourceManager",
                new RMMonitorExecutor(), metricWriteHelper);
        String xmlPath = getPath(taskArgs, RM_METRICS_XML_ARG, "monitors/HadoopMonitor/metrics-resource-manager.xml");
        conf.setMetricsXml(xmlPath, MetricConfig.class);
        conf.setConfigYml(configFilename, new ConfigReloader(), "resourceManagerMonitor");
        if (conf.isEnabled()) {
            conf.checkIfInitialized(ConfItem.CONFIG_YML, ConfItem.METRIC_PREFIX, ConfItem.METRIC_WRITE_HELPER);
        } else{
            logger.info("The Resource Manager Monitor is not enabled");
        }
        return conf;
    }

    private String getPath(Map<String, String> taskArgs, String pathKey, String defaultValue) {
        String path = taskArgs.get(pathKey);
        if (StringUtils.hasText(path)) {
            return path.trim();
        } else {
            return defaultValue;
        }
    }

    private class AmbariMonitorExecutor implements Runnable {

        public void run() {
            Map<String, ?> configYml = ambariMonitorConfig.getConfigYml();
            List<Map> servers = (List<Map>) configYml.get("servers");
            if (servers != null && !servers.isEmpty()) {
                for (Map server : servers) {
                    AmbariMetricsFetcherTask task = new AmbariMetricsFetcherTask(ambariMonitorConfig, server);
                    ambariMonitorConfig.getExecutorService().execute(task);
                }
            }
        }
    }

    private class RMMonitorExecutor implements Runnable {

        public void run() {
            Map<String, ?> configYml = resourceMgrMonitorConfig.getConfigYml();
            List<Map> servers = (List<Map>) configYml.get("servers");
            if (servers != null && !servers.isEmpty()) {
                for (Map server : servers) {
                    ResourceMgrMetricsFetcherTask task = new ResourceMgrMetricsFetcherTask(resourceMgrMonitorConfig, server);
                    resourceMgrMonitorConfig.getExecutorService().execute(task);
                }
            }
        }
    }


    private class ConfigReloader implements MonitorConfiguration.FileWatchListener {

        public void onFileChange(File file) {
            try {
                //loadConfigYml(file);
            } catch (Exception e) {
                logger.error("Error while loading the Ambari configuration" + file.getAbsolutePath(), e);
            }
        }
    }

    public TaskOutput execute(Map<String, String> taskArgs, TaskExecutionContext arg1)
            throws TaskExecutionException {
        try {
            init(taskArgs);
            logger.debug("Executing the HadoopMonitor with args {}", taskArgs);
            ambariMonitorConfig.executeTask();
            resourceMgrMonitorConfig.executeTask();
        } catch (Exception e) {
            logger.error("Exception while running the task", e);
            throw new TaskExecutionException(e);
        }
        return new TaskOutput("Execution Triggered");
    }

    public static String getConfigFilename(String filename) {
        if (filename == null) {
            return "";
        }
        // for absolute paths
        if (new File(filename).exists()) {
            return filename;
        }
        // for relative paths
        File jarPath = PathResolver.resolveDirectory(AManagedMonitor.class);
        String configFileName = "";
        if (!Strings.isNullOrEmpty(filename)) {
            configFileName = jarPath + File.separator + filename;
        }
        return configFileName;
    }

    public static String getImplementationVersion() {
        return HadoopMonitor.class.getPackage().getImplementationTitle();
    }
}
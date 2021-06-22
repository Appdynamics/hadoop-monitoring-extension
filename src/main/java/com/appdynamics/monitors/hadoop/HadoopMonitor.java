package com.appdynamics.monitors.hadoop;
/**
 * Copyright 2021 AppDynamics
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


import com.appdynamics.extensions.ABaseMonitor;
import com.appdynamics.extensions.TasksExecutionServiceProvider;
import com.appdynamics.extensions.conf.MonitorContextConfiguration;
import com.appdynamics.extensions.logging.ExtensionsLoggerFactory;
import com.appdynamics.extensions.util.AssertUtils;
import com.appdynamics.monitors.hadoop.Utility.Constants;

import com.appdynamics.monitors.hadoop.communicator.AmbariMetricsFetcherTask;
import com.appdynamics.monitors.hadoop.communicator.ResourceMgrMetricsFetcherTask;
import com.appdynamics.monitors.hadoop.input.MetricConfig;
import com.appdynamics.monitors.hadoop.input.MetricStats;
import com.google.common.base.Strings;

import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

public class HadoopMonitor extends ABaseMonitor{

    private static final Logger logger = ExtensionsLoggerFactory.getLogger(HadoopMonitor.class);

    private MonitorContextConfiguration monitorContextConfiguration;
    private Map<String,?> configYml;

    @Override
    protected String getDefaultMetricPrefix() {
        return Constants.DEFAULT_METRIC_PREFIX;
    }

    @Override
    public String getMonitorName() {
        return Constants.MONITOR_NAME;
    }

    @Override
    protected void doRun(TasksExecutionServiceProvider serviceProvider) {
        List<Map<String,?>> servers = (List<Map<String, ?>>) configYml.get(Constants.SERVERS);
        AssertUtils.assertNotNull(servers, "The servers section cannot be null");

        for(Map<String,?> server: servers) {
            try {
                if (!Strings.isNullOrEmpty((String) server.get("type")) && ((String) server.get("type")).equals(Constants.RESOURCE_MANAGER_MONITOR)) {
                    logger.info("Starting ResourceManagerMonitorTask for server " + (server.get(Constants.DISPLAY_NAME)));
                    Map<String, ?> resourceManagerMonitor = (Map<String, ?>) configYml.get(Constants.RESOURCE_MANAGER_MONITOR);
                    AssertUtils.assertNotNull(resourceManagerMonitor, "resourceManagerMonitor cannot be null or empty in config file");
                    MetricConfig rmMetricConfig = getMetricConfig(Constants.RESOURCE_MANAGER_MONITOR);
                    AssertUtils.assertNotNull(rmMetricConfig,"No configurations are provided for " + Constants.RESOURCE_MANAGER_MONITOR + " in metrics.xml file. Not collecting metrics for "+server.get(Constants.DISPLAY_NAME));
                    ResourceMgrMetricsFetcherTask resourceMgrMetricsFetcherTask = new ResourceMgrMetricsFetcherTask(monitorContextConfiguration, serviceProvider.getMetricWriteHelper(), server, resourceManagerMonitor, rmMetricConfig);
                    serviceProvider.submit("ResourceMgrMetricsFetcherTask", resourceMgrMetricsFetcherTask);
                } else if (!Strings.isNullOrEmpty((String) server.get("type")) && ((String) server.get("type")).equals(Constants.AMBARI_MONITOR)) {
                    logger.info("Starting AmbariMonitorTask for server " + (server.get(Constants.DISPLAY_NAME)));
                    Map<String, ?> ambariMonitor = (Map<String, ?>) configYml.get(Constants.AMBARI_MONITOR);
                    AssertUtils.assertNotNull(ambariMonitor, "ambariMonitor cannot be null or empty in config file");
                    MetricConfig ambariMetricConfig = getMetricConfig(Constants.AMBARI_MONITOR);
                    AssertUtils.assertNotNull(ambariMetricConfig,"No configurations are provided for " + Constants.AMBARI_MONITOR + " in metrics.xml file. Not collecting metrics for "+server.get(Constants.DISPLAY_NAME));
                    AmbariMetricsFetcherTask ambariMetricsFetcherTask = new AmbariMetricsFetcherTask(monitorContextConfiguration, serviceProvider.getMetricWriteHelper(), server, ambariMonitor, ambariMetricConfig);
                    serviceProvider.submit("AmbariMetricsFetcherTask", ambariMetricsFetcherTask);
                } else {
                    logger.info("Not starting task for server {} as type is incorrectly set in config file", server.get(Constants.DISPLAY_NAME));
                }
            } catch(Exception ex){
                logger.error("Error occurred while running task for server "+server.get(Constants.DISPLAY_NAME),ex);
            }
        }
    }

    @Override
    protected void initializeMoreStuff(Map<String, String> args) {
        monitorContextConfiguration = getContextConfiguration();
        configYml = monitorContextConfiguration.getConfigYml();
        AssertUtils.assertNotNull(configYml,"The config.yml is not available");
        this.getContextConfiguration().setMetricXml(args.get(Constants.METRIC_FILE), MetricStats.class);
    }

    @Override
    protected List<Map<String, ?>> getServers() {
        return (List<Map<String, ?>>) configYml.get(Constants.SERVERS);
    }

    protected MetricConfig getMetricConfig(String monitorName){
        MetricStats metricStats = (MetricStats) monitorContextConfiguration.getMetricsXml();
        for(MetricConfig metricConfig: metricStats.getMetricConfig()){
            if(metricConfig.getName().equals(monitorName)){
                return metricConfig;
            }
        }
        return null;
    }

//    public static void main(String[] args) throws TaskExecutionException {
//
//        HadoopMonitor monitor = new HadoopMonitor();
//        final Map<String, String> taskArgs = new HashMap<>();
//        taskArgs.put("config-file", "src/main/resources/conf/config.yml");
//        taskArgs.put("metric-file", "src/main/resources/conf/metrics.xml");
//        monitor.execute(taskArgs, null);
//    }
}
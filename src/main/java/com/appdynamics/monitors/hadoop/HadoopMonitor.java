/**
 * Copyright 2013 AppDynamics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.appdynamics.monitors.hadoop;

import com.appdynamics.extensions.PathResolver;
import com.appdynamics.extensions.util.MetricUtils;
import com.appdynamics.extensions.yml.YmlReader;
import com.appdynamics.monitors.hadoop.communicator.AmbariCommunicator;
import com.appdynamics.monitors.hadoop.communicator.HadoopCommunicator;
import com.appdynamics.monitors.hadoop.config.AmbariConfig;
import com.appdynamics.monitors.hadoop.config.Configuration;
import com.appdynamics.monitors.hadoop.config.ResourceManagerConfig;
import com.google.common.base.Strings;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class HadoopMonitor extends AManagedMonitor {
    public static final String CONFIG_ARG = "config-file";
    private static Logger logger = Logger.getLogger(HadoopMonitor.class);

    public HadoopMonitor() {
        String msg = "Using Monitor Version [" + getImplementationVersion() + "]";
        logger.info(msg);
        System.out.println(msg);
    }

    public TaskOutput execute(Map<String, String> taskArgs, TaskExecutionContext arg1)
            throws TaskExecutionException {
        if (taskArgs != null) {
            logger.info("Starting " + getImplementationVersion() + " Monitoring Task");
            String configFilename = getConfigFilename(taskArgs.get(CONFIG_ARG));
            try {
                Configuration config = YmlReader.readFromFile(configFilename, Configuration.class);

                if (config.isResourceManagerMonitor()) {
                    ResourceManagerConfig resourceManagerConfig = config.getResourceManagerConfig();
                    boolean hasResourceManager = determineIfHadoopHasResourceManger(resourceManagerConfig.getHadoopVersion());
                    Map<String, Object> hadoopMetrics = new HashMap<String, Object>();
                    if (hasResourceManager) {
                        HadoopCommunicator hadoopCommunicator = new HadoopCommunicator(resourceManagerConfig);
                        hadoopCommunicator.populate(hadoopMetrics);
                        printResourceManagerMetrics(config.getMetricPathPrefix() + "Resource Manager|", hadoopMetrics);
                    } else {
                        logger.debug("Hadoop version doesn't have Resource Manager");
                    }
                } else {
                    logger.debug("Monitor is running with collecting Resource Manager metrics disabled");
                }


                if (config.isAmbariMonitor()) {
                    AmbariConfig ambariConfig = config.getAmbariConfig();
                    AmbariCommunicator ambariCommunicator = new AmbariCommunicator(ambariConfig);
                    Map<String, Number> ambariMetrics = ambariCommunicator.fetchAmbariMetrics();
                    printAmbariMetrics(config.getMetricPathPrefix() + "Ambari|", ambariMetrics);
                } else {
                    logger.debug("Monitor is running with collecting Ambari metrics disabled");
                }

                logger.info("Hadoop Monioring Task completed successfully");
                return new TaskOutput("Hadoop Monioring Task completed successfully");
            } catch (Exception e) {
                logger.error("Metrics collection failed ", e);
            }
        }
        throw new TaskExecutionException("Hadoop Monioring Task completed with failures");
    }

    private boolean determineIfHadoopHasResourceManger(String hadoopVersion) {
        boolean hasResourceManager = false;
        String[] hadoopVersionSplit = hadoopVersion.trim().split("\\.");
        try {
            int majorVer = Integer.parseInt(hadoopVersionSplit[0]);
            if (majorVer == 0) {
                if (Integer.parseInt(hadoopVersionSplit[1]) >= 23) {
                    hasResourceManager = true;
                }
            } else if (majorVer >= 2) {
                hasResourceManager = true;
            }
        } catch (NumberFormatException e) {
            hasResourceManager = false;
            logger.error("Invalid Hadoop version " + hadoopVersion);
        }
        return hasResourceManager;
    }

    private void printResourceManagerMetrics(String metricPathPrefix, Map<String, Object> hadoopMetrics) {
        for (Map.Entry<String, Object> entry : hadoopMetrics.entrySet()) {
            printMetric(metricPathPrefix + entry.getKey(), entry.getValue());
        }
    }

    private void printAmbariMetrics(String metricPathPrefix, Map<String, Number> ambariMetrics) {
        for (Map.Entry<String, Number> entry : ambariMetrics.entrySet()) {
            printMetric(metricPathPrefix + entry.getKey(), entry.getValue());
        }
    }

    private void printMetric(String metricName, Object metricValue) {
        try {
            MetricWriter metricWriter = getMetricWriter(metricName,
                    MetricWriter.METRIC_AGGREGATION_TYPE_AVERAGE,
                    MetricWriter.METRIC_TIME_ROLLUP_TYPE_AVERAGE,
                    MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_INDIVIDUAL
            );
            if (metricValue != null && metricValue instanceof Number) {
                String value = MetricUtils.toWholeNumberString(metricValue);
                metricWriter.printMetric(value);
                if (logger.isDebugEnabled()) {
                    logger.debug("Metric: " + metricName + " value: "+ metricValue+" -> " + value);
                }
            }
        } catch (Exception e) {
            logger.error("Exception while printing the metric: " + metricName + " value: "+ metricValue, e);
        }
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
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
import com.appdynamics.monitors.hadoop.parser.Parser;
import com.google.common.base.Strings;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.apache.log4j.Logger;
import org.dom4j.DocumentException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class HadoopMonitor extends AManagedMonitor {
    public static final String CONFIG_ARG = "config-file";
    private static Logger logger = Logger.getLogger(HadoopMonitor.class);
    Parser xmlParser;

    public HadoopMonitor() {
        String msg = "Using Monitor Version [" + getImplementationVersion() + "]";
        logger.info(msg);
        System.out.println(msg);
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

    public TaskOutput execute(Map<String, String> taskArgs, TaskExecutionContext arg1)
            throws TaskExecutionException {
        if (taskArgs != null) {
            logger.info("Starting HadoopMonitor Task");
            String configFilename = getConfigFilename(taskArgs.get(CONFIG_ARG));
            try {
                Configuration config = YmlReader.readFromFile(configFilename, Configuration.class);

                if (xmlParser == null) {
                    String xml = taskArgs.get("properties-path");
                    try {
                        xmlParser = new Parser(logger, xml);
                    } catch (DocumentException e) {
                        logger.error("Cannot read '" + xml + "'. Monitor is running without metric filtering\n" +
                                "Error: ", e);
                        xmlParser = new Parser(logger);
                    }
                }
                boolean hasResourceManager = determineIfHadoopHasResourceManger(config.getHadoopVersion());
                Map<String, Object> hadoopMetrics = new HashMap<String, Object>();
                if (hasResourceManager) {
                    ResourceManagerConfig resourceManagerConfig = config.getResourceManagerConfig();
                    HadoopCommunicator hadoopCommunicator = new HadoopCommunicator(resourceManagerConfig, xmlParser);
                    hadoopCommunicator.populate(hadoopMetrics);
                } else {
                    logger.warn("Monitor is running without Resource Manager metrics");
                }


                Map<String, Number> ambariMetrics = new HashMap<String, Number>();
                if (config.isAmbariMonitor()) {
                    AmbariConfig ambariConfig = config.getAmbariConfig();
                    AmbariCommunicator ambariCommunicator = new AmbariCommunicator(ambariConfig, xmlParser);
                    ambariMetrics = ambariCommunicator.fetchAmbariMetrics();
                }

                printHadoopMetrics(config.getMetricPathPrefix(), hadoopMetrics, ambariMetrics);

                logger.info("Hadoop Monioring Task completed successfully");
                return new TaskOutput("Hadoop Monioring Task completed successfully");
            } catch (Exception e) {
                logger.error("Metrics collection failed", e);
            }
        }
        throw new TaskExecutionException("Hadoop Monioring Task completed with failures");
    }

    private void printHadoopMetrics(String metricPathPrefix, Map<String, Object> hadoopMetrics, Map<String, Number> ambariMetrics) {
        try {
            for (Map.Entry<String, Object> entry : hadoopMetrics.entrySet()) {
                printMetric(metricPathPrefix + "Resource Manager|" + entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Number> entry : ambariMetrics.entrySet()) {
                printMetric(metricPathPrefix + "Ambari|" + entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            logger.error("Error printing metrics: ", e);
        }
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

    private void printMetric(String metricName, Object metricValue) {
        MetricWriter metricWriter = getMetricWriter(metricName,
                MetricWriter.METRIC_AGGREGATION_TYPE_AVERAGE,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_AVERAGE,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_INDIVIDUAL
        );
        if (metricValue instanceof Number) {
            String value = MetricUtils.toWholeNumberString(metricValue);
            metricWriter.printMetric(value);
        }
    }
}
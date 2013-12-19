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

import com.appdynamics.monitors.hadoop.communicator.AmbariCommunicator;
import com.appdynamics.monitors.hadoop.communicator.HadoopCommunicator;
import com.appdynamics.monitors.hadoop.parser.Parser;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.apache.log4j.Logger;
import org.dom4j.DocumentException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class HadoopMonitor extends AManagedMonitor
{
    Parser xmlParser;

    private String metricPath = "Custom Metrics|Hadoop|";
    HadoopCommunicator hadoopCommunicator;
    AmbariCommunicator ambariCommunicator;

    private static Logger logger = Logger.getLogger(HadoopMonitor.class);

    public TaskOutput execute(Map<String, String> args, TaskExecutionContext arg1)
            throws TaskExecutionException
    {
        logger.info("Executing HadoopMonitor");

        try {
            boolean hasResourceManager = false;
            String hadoopVersion = args.get("hadoop-version");
            String[] hadoopVersionSplit = hadoopVersion.split("\\.");

            try {
                int majorVer = Integer.parseInt(hadoopVersionSplit[0]);
                if (majorVer == 0) {
                    if (Integer.parseInt(hadoopVersionSplit[1]) >= 23) {
                        hasResourceManager = true;
                    }
                } else if (majorVer >= 2){
                    hasResourceManager = true;
                }
            } catch (NumberFormatException e){
                hasResourceManager = false;
                logger.error("Invalid Hadoop version '" + hadoopVersion + "'");
            }

            if (!args.get("metric-path").equals("")){
                metricPath = args.get("metric-path");
                if (!metricPath.endsWith("|")){
                    metricPath += "|";
                }
                metricPath += "Hadoop|";
            }

            if (xmlParser == null){
                String xml = args.get("properties-path");
                try {
                    xmlParser = new Parser(logger, xml);
                } catch (DocumentException e) {
                    logger.error("Cannot read '" + xml + "'. Monitor is running without metric filtering\n"+
                            "Error: " + stackTraceToString(e));
                    xmlParser = new Parser(logger);
                }
            }

            Map<String, Object> hadoopMetrics = new HashMap<String, Object>();
            Map<String, Object> ambariMetrics = new HashMap<String, Object>();

            if (hasResourceManager){
                String host = args.get("host");
                String port = args.get("port");
                hadoopCommunicator = new HadoopCommunicator(host,port,logger,xmlParser);
                hadoopCommunicator.populate(hadoopMetrics);
            } else {
                logger.warn("Monitor is running without Resource Manager metrics");
            }
            if (args.get("ambari-monitor").equals("true")){
                String ambariHost = args.get("ambari-host");
                String ambariPort = args.get("ambari-port");
                String ambariUser = args.get("ambari-user");
                String ambariPassword = args.get("ambari-password");
                ambariCommunicator = new AmbariCommunicator(ambariHost, ambariPort, ambariUser, ambariPassword, logger, xmlParser);
                ambariCommunicator.populate(ambariMetrics);
            }

            try{
                for (Map.Entry<String, Object> entry : hadoopMetrics.entrySet()){
                    printMetric(metricPath + "Resource Manager|" + entry.getKey(), entry.getValue(),
                            MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                            MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                            MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
                }

                for (Map.Entry<String, Object> entry : ambariMetrics.entrySet()){
                    printMetric(metricPath + entry.getKey(), entry.getValue(),
                            MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                            MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                            MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
                }
            } catch (Exception e){
                logger.error("Error printing metrics: " + stackTraceToString(e));
            }

            return new TaskOutput("Hadoop Metric Upload Complete");
        } catch (Exception e) {
            logger.error(stackTraceToString(e));
            return new TaskOutput("Hadoop Metric Upload Failed");
        }
    }


    private void printMetric(String metricName, Object metricValue, String aggregation, String timeRollup, String cluster)
    {
        MetricWriter metricWriter = getMetricWriter(metricName,
                aggregation,
                timeRollup,
                cluster
        );
        if (metricValue instanceof Double){
            metricWriter.printMetric(String.valueOf(Math.round((Double)metricValue)));
        } else if (metricValue instanceof Float) {
            metricWriter.printMetric(String.valueOf(Math.round((Float)metricValue)));
        } else {
            metricWriter.printMetric(String.valueOf(metricValue));
        }
    }

    private String stackTraceToString(Exception e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
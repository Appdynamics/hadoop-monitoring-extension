/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

package com.appdynamics.monitors.hadoop.communicator;

import com.appdynamics.extensions.AMonitorTaskRunnable;
import com.appdynamics.extensions.MetricWriteHelper;
import com.appdynamics.extensions.conf.MonitorContextConfiguration;
import com.appdynamics.extensions.http.HttpClientUtils;
import com.appdynamics.extensions.http.UrlBuilder;
import com.appdynamics.extensions.logging.ExtensionsLoggerFactory;
import com.appdynamics.extensions.util.*;
import com.appdynamics.monitors.hadoop.Utility.Constants;
import com.appdynamics.monitors.hadoop.Utility.MetricUtils;
import com.appdynamics.monitors.hadoop.input.Metric;
import com.appdynamics.monitors.hadoop.input.MetricConfig;
import com.appdynamics.monitors.hadoop.input.Stat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by abey.tom on 9/22/16.
 */
public class ResourceMgrMetricsFetcherTask implements AMonitorTaskRunnable {
    private static final Logger logger = ExtensionsLoggerFactory.getLogger(ResourceMgrMetricsFetcherTask.class);

    public static final String[] KNOWN_STATUS = {"UNDEFINED", "SUCCEEDED", "FAILED", "KILLED"};
    public static final String[] KNOWN_STATES = {"NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING", "FINISHED", "FAILED", "KILLED"};

    private MonitorContextConfiguration configuration;
    private MetricWriteHelper metricWriteHelper;
    private Map<String, ?> server;
    private Map<String,?> resourceManagerMonitor;
    private String metricPrefix;
    private MetricConfig metricConfig;

    public ResourceMgrMetricsFetcherTask(MonitorContextConfiguration configuration, MetricWriteHelper metricWriteHelper, Map<String, ?> server, Map<String,?> resourceManagerMonitor, MetricConfig metricConfig) {
        this.configuration = configuration;
        this.metricWriteHelper=metricWriteHelper;
        this.server = server;
        this.resourceManagerMonitor=resourceManagerMonitor;
        this.metricPrefix=configuration.getMetricPrefix()+ Constants.SEPARATOR+Constants.RESOURCE_MANAGER_MONITOR+Constants.SEPARATOR+server.get(Constants.DISPLAY_NAME);
        this.metricConfig=metricConfig;
    }

    @Override
    public void run() {
        runTask();
    }

    public void runTask() {
        Stat[] stats = metricConfig.getStats();
        if (stats != null && stats.length > 0) {
            for (Stat stat : stats) {
                String urlPath = stat.getUrl();
                if (urlPath != null) {
                    String fullUrl = UrlBuilder.fromYmlServerConfig(server)
                            .path(urlPath).build();
                    JsonNode response = getResponseAsJson(fullUrl);
                    if (response != null) {
                        parseResponse(stat, response);
                    } else {
                        logger.error("The url [{}] returned no data ", fullUrl);
                    }
                }
            }
        }
        printSchedulerMetrics();
        printJobTypeMonitoring();
    }

    protected JsonNode getResponseAsJson(String fullUrl) {
        return HttpClientUtils.getResponseAsJson(configuration.getContext().getHttpClient()
                , fullUrl, JsonNode.class, Collections.singletonMap("Accept", "application/json"));
    }

    protected void printJobTypeMonitoring() {
        Map<String, ?> config = configuration.getConfigYml();
        int monitoringTimePeriod = YmlUtils.getInt(resourceManagerMonitor.get("monitoringTimePeriod"), 15);
        long monitoringPeriod = System.currentTimeMillis() - monitoringTimePeriod * 60 * 1000;
        List<Map<String, ?>> applications = (List<Map<String, ?>>) resourceManagerMonitor.get("applications");
        if (applications != null) {
            for (Map<String, ?> application : applications) {
                String applicationType = (String) application.get("type");
                String prefix = StringUtils.concatMetricPath(metricPrefix, "Current Apps", applicationType);
                JsonNode response = getAppsOfTypeResponse(monitoringPeriod, application);
                JsonNode apps = JsonUtils.getNestedObject(response, "apps", "app");
                List<String> names = (List<String>) application.get("names");
                if (apps instanceof ArrayNode) {
                    ArrayNode jsonNodes = (ArrayNode) apps;
                    GroupCounter<String> stateCounter = new GroupCounter<String>();
                    GroupCounter<String> statusCounter = new GroupCounter<String>();
                    for (JsonNode jsonNode : jsonNodes) {
                        String name = JsonUtils.getTextValue(jsonNode, "name");
                        if (matches(name, names)) {
                            String state = JsonUtils.getTextValue(jsonNode, "state");
                            stateCounter.increment(state);
                            String status = JsonUtils.getTextValue(jsonNode, "finalStatus");
                            statusCounter.increment(status);
                        } else {
                            logger.debug("The application name [{}] didnt match with the filter {}", name, names);
                        }

                    }
                    printAppTypeStates(StringUtils.concatMetricPath(prefix, "Final Status"),
                            statusCounter, KNOWN_STATUS);
                    printAppTypeStates(StringUtils.concatMetricPath(prefix, "State"),
                            stateCounter, KNOWN_STATES);
                } else {
                    logger.warn("There are no jobs of type {} found between [{}] and now", applicationType, new Date(monitoringPeriod));
                }
            }
        }
    }

    private boolean matches(String name, List<String> regexes) {
        if (regexes != null) {
            for (String regex : regexes) {
                if (name.matches(regex)) {
                    return true;
                }
            }
        } else {
            return true;
        }
        return false;
    }

    private void printAppTypeStates(String prefix, GroupCounter<String> statusCounter, String[] constants) {
        for (String stat : constants) {
            Long value = statusCounter.get(stat);
            BigDecimal val;
            if (value == null) {
                val = new BigDecimal("0");
            } else {
                val = new BigDecimal(value);
            }
            String metricPath = StringUtils.concatMetricPath(prefix, stat);
            metricWriteHelper.printMetric(metricPath, val, "OBS.CUR.COL");
        }
    }

    /**
     * http://192.168.1.216:8088/ws/v1/cluster/apps?monitoringTimePeriod=15&applicationTypes=MAPREDUCE
     *
     * @param monitoringPeriod
     * @param application
     */
    private JsonNode getAppsOfTypeResponse(long monitoringPeriod, Map<String, ?> application) {
        String type = (String) application.get("type");
        UrlBuilder path = UrlBuilder.fromYmlServerConfig(server)
                .path("/ws/v1/cluster/apps")
                .query("applicationTypes", type)
                .query("startedTimeBegin", String.valueOf(monitoringPeriod));
        return getResponseAsJson(path.build());
    }

    private void printSchedulerMetrics() {
        JsonNode response = getSchedulerResponse();
        if (response != null) {
            JsonNode json = JsonUtils.getNestedObject(response, "scheduler", "schedulerInfo");
            String schedulerType = JsonUtils.getTextValue(json, "type");
            logger.debug("The scheduler type is {}", schedulerType);
            if ("capacityScheduler".equals(schedulerType)) {
                String label = "Capacity Scheduler|Queues";
                Stat stat = getStatsByName("capacitySchedulerQueue");
                printCapacitySchedulerQueueMetrics(json, stat, label);
            } else if ("fifoScheduler".equals(schedulerType)) {
                String label = "FIFO Scheduler|Queues";
                Stat stat = getStatsByName("fifoScheduler");
                stat.setLabel(label);
                parseResponse(stat, json);
            } else if ("fairScheduler".equals(schedulerType)) {
                JsonNode rootQueue = json.get("rootQueue");
                if (rootQueue != null) {
                    String label = "Fair Scheduler|Queues";
                    Stat stat = getStatsByName("fairSchedulerQueue");
                    printFairSchedulerQueueMetrics(rootQueue, stat, label);
                }
            } else {
                logger.error("Unknown scheduler type {} from teh response {}", schedulerType, json);
            }
        }
    }

    protected JsonNode getSchedulerResponse() {
        UrlBuilder path = UrlBuilder.fromYmlServerConfig(server).path("/ws/v1/cluster/scheduler");
        return getResponseAsJson(path.build());
    }

    private void printCapacitySchedulerQueueMetrics(JsonNode parentQueue, Stat stat, String label) {
        stat.setLabel(label);
        parseResponse(stat, parentQueue);
        JsonNode childQueues = JsonUtils.getNestedObject(parentQueue, "queues", "queue");
        if (childQueues instanceof ArrayNode) {
            ArrayNode queues = (ArrayNode) childQueues;
            for (JsonNode childQueue : queues) {
                String qName = JsonUtils.getTextValue(parentQueue, "queueName");
                printCapacitySchedulerQueueMetrics(childQueue, stat, StringUtils.concatMetricPath(label, qName, "Queues"));
            }
        } else if (childQueues != null) {
            logger.warn("Expecting an Array. However found {}", childQueues);
        }
    }

    private void printFairSchedulerQueueMetrics(JsonNode parentQueue, Stat stat, String label) {
        stat.setLabel(label);
        parseResponse(stat, parentQueue);
        JsonNode childQueues = parentQueue.get("childQueues");
        if (childQueues instanceof ArrayNode) {
            ArrayNode queues = (ArrayNode) childQueues;
            for (JsonNode childQueue : queues) {
                String qName = JsonUtils.getTextValue(parentQueue, "queueName");
                printFairSchedulerQueueMetrics(childQueue, stat, StringUtils.concatMetricPath(label, qName, "Queues"));
            }
        } else if (childQueues != null) {
            logger.warn("Expecting an Array. However found {}", childQueues);
        }
    }

    private Stat getStatsByName(String name) {
        Stat[] stats = metricConfig.getStats();
        if (stats != null) {
            for (Stat stat : stats) {
                if (name != null && name.equals(stat.getName())) {
                    return stat;
                }
            }
        }
        return null;
    }

    private void parseResponse(Stat stat, JsonNode response) {
        JsonNode children;
        if (stat.getChildren() != null) {
            String[] split = stat.getChildren().split("\\|");
            children = JsonUtils.getNestedObject(response, split);
        } else {
            children = response;
        }
        if (children instanceof ArrayNode) {
            ArrayNode nodes = (ArrayNode) children;
            for (JsonNode node : nodes) {
                reportMetrics(node, stat);
            }
        } else if (children instanceof JsonNode) {
            reportMetrics(children, stat);
        }
    }

    private void reportMetrics(JsonNode node, Stat stat) {
        String statPrefix = StringUtils.concatMetricPath(metricPrefix, stat.getLabel());
        Metric[] metrics = stat.getMetrics();
        if (metrics != null) {
            for (Metric metric : metrics) {
                String attr = metric.getAttr();
                String metricValue = JsonUtils.getTextValue(node, attr.split("\\|"));
                metricValue = metric.convertValue(attr, metricValue, metricConfig);
                if (NumberUtils.isNumber(metricValue)) {
                    BigDecimal val = MetricUtils.multiplyAndRound(metricValue, metric.getMultiplier());
                    String label = getLabel(metric, node);
                    if (label != null) {
                        String metricPath = StringUtils.concatMetricPath(statPrefix, label);
                        metricWriteHelper.printMetric(metricPath, val, getMetricType(stat, metric));
                    }
                } else {
                    logger.debug("The metric path {} didnt return any value", attr);
                }
            }
        }
    }

    private String getMetricType(Stat stat, Metric metric) {
        if (metric.getMetricType() != null) {
            return metric.getMetricType();
        } else if (stat.getMetricType() != null) {
            return stat.getMetricType();
        } else {
            return "AVG.AVG.COL";
        }
    }

    private String getLabel(Metric metric, JsonNode node) {
        String label = metric.getLabel();
        if (label != null) {
            String[] split = label.split("\\|");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < split.length; i++) {
                String segment = split[i];
                if (segment.startsWith("{") && segment.endsWith("}")) {
                    String variable = segment.substring(1, segment.length() - 1);
                    String textValue = JsonUtils.getTextValue(node, variable);
                    if (textValue != null) {
                        textValue = textValue.replace(":", "-");
                        sb.append(textValue).append("|");
                    } else {
                        logger.warn("Cannot resolve the value of dynamic path {} from label {}", segment, label);
                        return null;
                    }
                } else {
                    sb.append(segment).append("|");
                }
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        } else {
            return metric.getAttr();
        }
    }



    @Override
    public void onTaskComplete() {
        logger.info("Completed ResourceMgrMetricsFetcherTask for server "+server.get(Constants.DISPLAY_NAME));
    }

}

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

package com.appdynamics.monitors.hadoop.communicator;

import com.appdynamics.TaskInputArgs;
import com.appdynamics.extensions.http.SimpleHttpClient;
import com.appdynamics.monitors.hadoop.config.AmbariConfig;
import com.appdynamics.monitors.hadoop.hadoopexception.AmbariMonitorException;
import com.appdynamics.monitors.hadoop.util.HadoopMonitorUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AmbariCommunicator {
    private AmbariConfig config;
    private Logger logger = Logger.getLogger(AmbariCommunicator.class);
    private ExecutorService executor;
    private Map<String, Number> ambariMetrics;
    private SimpleHttpClient httpClient;

    /**
     * Constructs a new AmbariCommunicator. Metrics can be collected by calling {@link #fetchAmbariMetrics}
     * Only metrics that match the conditions in <code>xmlParser</code> are collected.
     */
    public AmbariCommunicator(AmbariConfig config) {
        this.config = config;
        ambariMetrics = Maps.newHashMap();
        executor = Executors.newFixedThreadPool(config.getNumberOfThreads());
        httpClient = new SimpleHttpClient(buildHttpClientArguments(), null, null, true);
    }


    /**
     * Populates <code>metrics</code> Map with all numeric Ambari clusters metrics.
     */
    public Map<String, Number> fetchAmbariMetrics() throws AmbariMonitorException {
        try {
            JsonNode clusterResponseNode = new AmbariTask(httpClient, "", HadoopMonitorUtil.AMBARI_CLUSTER_URI).call();
            JsonNode clusters = clusterResponseNode.path("items");
            for (JsonNode cluster : clusters) {
                String clusterName = cluster.path("Clusters").path("cluster_name").asText();
                if (isIncludeCluster(clusterName)) {
                    JsonNode servicesNode = cluster.path("services");
                    JsonNode hostsNode = cluster.path("hosts");

                    CompletionService<JsonNode> threadPool = new ExecutorCompletionService<JsonNode>(executor);
                    int count = 0;
                    for (JsonNode service : servicesNode) {
                        String serviceName = service.path("ServiceInfo").path("service_name").asText();
                        if (isIncludeService(serviceName)) {
                            String url = service.path("href").asText();
                            threadPool.submit(new AmbariTask(httpClient, url, ""));
                            count++;
                        }
                    }
                    for (; count > 0; count--) {
                        getServiceMetrics(threadPool.take().get(), clusterName + HadoopMonitorUtil.METRIC_SEPARATOR + "services");
                    }

                    for (JsonNode host : hostsNode) {
                        String hostName = host.path("Hosts").path("host_name").asText();
                        if (isIncludeHost(hostName)) {
                            String url = host.path("href").asText();
                            threadPool.submit(new AmbariTask(httpClient, url, ""));
                            count++;
                        }
                    }
                    for (; count > 0; count--) {
                        getHostMetrics(threadPool.take().get(), clusterName + HadoopMonitorUtil.METRIC_SEPARATOR + "hosts");
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error fetching Ambari Metrics ", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
            if (executor != null && !executor.isShutdown()) {
                executor.shutdown();
            }
        }
        return ambariMetrics;
    }

    private Map<String, String> buildHttpClientArguments() {
        Map<String, String> clientArgs = new HashMap<String, String>();
        clientArgs.put(TaskInputArgs.HOST, config.getHost());
        clientArgs.put(TaskInputArgs.PORT, String.valueOf(config.getPort()));
        clientArgs.put(TaskInputArgs.USER, config.getUsername());
        clientArgs.put(TaskInputArgs.PASSWORD, config.getPassword());
        return clientArgs;
    }

    private void getServiceMetrics(JsonNode serviceNode, String metricPath) {
        try {
            JsonNode serviceInfo = serviceNode.get("ServiceInfo");
            String serviceName = serviceInfo.get("service_name").asText();
            String serviceState = serviceInfo.get("state").asText();

            ambariMetrics.put(metricPath + HadoopMonitorUtil.METRIC_SEPARATOR + serviceName + "|state", getIndexOfState(serviceState));

            JsonNode components = serviceNode.get("components");

            CompletionService<JsonNode> threadPool = new ExecutorCompletionService<JsonNode>(executor);
            int count = 0;
            for (JsonNode component : components) {
                String url = component.path("href").asText();
                threadPool.submit(new AmbariTask(httpClient, url, ""));
                count++;
            }
            for (; count > 0; count--) {
                getComponentMetrics(threadPool.take().get(), metricPath + HadoopMonitorUtil.METRIC_SEPARATOR + serviceName);
            }

            JsonNode metricsNode = serviceNode.get("metrics");
            populateMetrics(metricsNode, metricPath + HadoopMonitorUtil.METRIC_SEPARATOR + serviceName + "|Metrics");

        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void getComponentMetrics(JsonNode componentNode, String metricPath) {
        try {
            JsonNode componentInfo = componentNode.get("ServiceComponentInfo");
            String componentName = componentInfo.get("component_name").asText();
            String componentState = componentInfo.get("state").asText();

            ambariMetrics.put(metricPath + HadoopMonitorUtil.METRIC_SEPARATOR + componentName + "|state", getIndexOfState(componentState));

            JsonNode metricsNode = componentNode.get("metrics");
            populateMetrics(metricsNode, metricPath + HadoopMonitorUtil.METRIC_SEPARATOR + componentName + "|Metrics");
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void getHostMetrics(JsonNode hostNode, String metricPath) {
        try {
            JsonNode hostInfo = hostNode.get("Hosts");
            String hostName = hostInfo.get("host_name").asText();
            String hostState = hostInfo.get("host_state").asText();

            List<String> states = new ArrayList<String>();
            states.add("INIT");
            states.add("WAITING_FOR_HOST_STATUS_UPDATES");
            states.add("HEALTHY");
            states.add("HEARTBEAT_LOST");
            states.add("UNHEALTHY");

            metricPath += "|" + hostName + "|";

            ambariMetrics.put(metricPath + "state", states.indexOf(hostState));

            JsonNode metricsNode = hostNode.get("metrics");
            populateMetrics(metricsNode, metricPath + "Metrics");

            JsonNode hostComponents = hostNode.get("host_components");
            CompletionService<JsonNode> threadPool = new ExecutorCompletionService<JsonNode>(executor);
            int count = 0;
            for (JsonNode hostComponent : hostComponents) {
                threadPool.submit(new AmbariTask(httpClient, hostComponent.get("href").asText(), ""));
                count++;
            }
            for (; count > 0; count--) {
                getHostComponentMetrics(threadPool.take().get(), metricPath);
            }

        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void getHostComponentMetrics(JsonNode hostComponentNode, String metricPath) {
        try {
            JsonNode componentInfo = hostComponentNode.get("HostRoles");
            String componentName = componentInfo.get("component_name").asText();
            String componentState = componentInfo.get("state").asText();

            metricPath = metricPath + componentName + "|";

            ambariMetrics.put(metricPath + "state", getIndexOfState(componentState));

            JsonNode metricsNode = hostComponentNode.get("metrics");
            populateMetrics(metricsNode, metricPath + "Metrics");

        } catch (Exception e) {
            logger.error(e);
        }
    }

    private int getIndexOfState(String state) {
        List<String> states = new ArrayList<String>();
        states.add("INIT");
        states.add("INSTALLING");
        states.add("INSTALL_FAILED");
        states.add("INSTALLED");
        states.add("STARTING");
        states.add("STARTED");
        states.add("STOPPING");
        states.add("UNINSTALLING");
        states.add("UNINSTALLED");
        states.add("WIPING_OUT");
        states.add("UPGRADING");
        states.add("MAINTENANCE");
        states.add("UNKNOWN");

        return states.indexOf(state);
    }

    private void populateMetrics(JsonNode metricsNode, String metricPath) {
        try {
            if (metricsNode != null) {
                Iterator<Map.Entry<String, JsonNode>> fields = metricsNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String fieldName = field.getKey();
                    JsonNode fieldValue = field.getValue();
                    if (fieldValue.isObject()) {
                        populateMetrics(field.getValue(), metricPath + "|" + fieldName);
                    } else if (fieldValue.isArray()) {
                        Iterator<JsonNode> arrayFields = field.getValue().elements();
                        while (arrayFields.hasNext()) {
                            JsonNode elementNode = arrayFields.next();
                            populateMetrics(elementNode, metricPath + "|" + fieldName);
                        }
                    } else if (fieldValue.isNumber()) {
                        String metricName = metricPath + "|" + fieldName;
                        // removing boottime (not required)
                        if (!"boottime".equals(fieldName) && !"part_max_used".equals(fieldName)) {
                            if (fieldName.startsWith("load_")) {  //convert all load factors to percentage integers
                                if (fieldValue.isDouble()) {
                                    ambariMetrics.put(metricName, fieldValue.asDouble() * 100);
                                } else { // //if Ganglia is down, load metrics are Long
                                    ambariMetrics.put(metricName, fieldValue.asDouble());
                                }
                            } else {
                                ambariMetrics.put(metricName, fieldValue.asDouble());
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error while parsing metrics", e);
        }
    }

    private boolean isIncludeCluster(String cluster) {
        Set<String> includeClusters = config.getIncludeClusters();
        if (includeClusters.isEmpty() || includeClusters.contains(cluster)) {
            return true;
        }
        return false;
    }

    private boolean isIncludeHost(String host) {
        Set<String> includeHosts = config.getIncludeHosts();
        if (includeHosts.isEmpty() || includeHosts.contains(host)) {
            return true;
        }
        return false;
    }

    private boolean isIncludeService(String service) {
        Set<String> includeServices = config.getIncludeServices();
        if (includeServices.isEmpty() || includeServices.contains(service)) {
            return true;
        }
        return false;
    }
}
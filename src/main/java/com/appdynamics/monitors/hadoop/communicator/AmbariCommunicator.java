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
import com.appdynamics.monitors.hadoop.parser.Parser;
import com.appdynamics.monitors.hadoop.util.HadoopConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AmbariCommunicator {
    private AmbariConfig config;
    private Logger logger = Logger.getLogger(AmbariCommunicator.class);
    private Parser xmlParser;
    private ExecutorService executor;
    private Map<String, Number> ambariMetrics;
    private SimpleHttpClient httpClient;

    /**
     * Constructs a new AmbariCommunicator. Metrics can be collected by calling {@link #fetchAmbariMetrics}
     * Only metrics that match the conditions in <code>xmlParser</code> are collected.
     *
     * @param xmlParser XML parser for metric filtering
     */
    public AmbariCommunicator(AmbariConfig config, Parser xmlParser) {
        this.config = config;
        this.xmlParser = xmlParser;
        ambariMetrics = Maps.newHashMap();
        executor = Executors.newFixedThreadPool(xmlParser.getThreadLimit());
        httpClient = new SimpleHttpClient(buildHttpClientArguments(), null, null, true);
    }

    /**
     * Populates <code>metrics</code> Map with all numeric Ambari clusters metrics.
     */
    public Map<String, Number> fetchAmbariMetrics() throws AmbariMonitorException {
        try {
            JsonNode clusterResponseNode = new AmbariTask(httpClient, "", HadoopConstants.AMBARI_CLUSTER_URI).call();
            JsonNode clusters = clusterResponseNode.path("items");
            for (JsonNode cluster : clusters) {
                String clusterName = cluster.path("Clusters").path("cluster_name").asText();
                if (xmlParser.isIncludeCluster(clusterName)) {
                    JsonNode servicesNode = cluster.path("services");
                    JsonNode hostsNode = cluster.path("hosts");

                    CompletionService<JsonNode> threadPool = new ExecutorCompletionService<JsonNode>(executor);
                    int count = 0;
                    for (JsonNode service : servicesNode) {
                        String serviceName = service.path("ServiceInfo").path("service_name").asText();
                        if (xmlParser.isIncludeService(serviceName)) {
                            String url = service.path("href").asText();
                            threadPool.submit(new AmbariTask(httpClient, url, ""));
                            count++;
                        }
                    }
                    for (; count > 0; count--) {
                        getServiceMetrics(threadPool.take().get(), clusterName + HadoopConstants.METRIC_SEPARATOR + "services");
                    }

                    for (JsonNode host : hostsNode) {
                        String hostName = host.path("Hosts").path("host_name").asText();
                        if (xmlParser.isIncludeHost(hostName)) {
                            String url = host.path("href").asText();
                            threadPool.submit(new AmbariTask(httpClient, url, ""));
                            count++;
                        }
                    }
                    for (; count > 0; count--) {
                        getHostMetrics(threadPool.take().get(), clusterName + HadoopConstants.METRIC_SEPARATOR + "hosts");
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e);
            throw new AmbariMonitorException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
            if (executor != null) {
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
            ambariMetrics.put(metricPath + HadoopConstants.METRIC_SEPARATOR + serviceName + "|state", states.indexOf(serviceState));

            JsonNode components = serviceNode.get("components");

            CompletionService<JsonNode> threadPool = new ExecutorCompletionService<JsonNode>(executor);
            int count = 0;
            for (JsonNode component : components) {
                String componentName = component.get("ServiceComponentInfo").get("component_name").asText();
                if (xmlParser.isIncludeServiceComponent(serviceName, componentName)) {
                    String url = component.path("href").asText();
                    threadPool.submit(new AmbariTask(httpClient, url, ""));
                    count++;
                }
            }
            for (; count > 0; count--) {
                getComponentMetrics(threadPool.take().get(), metricPath + HadoopConstants.METRIC_SEPARATOR + serviceName);
            }

        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void getComponentMetrics(JsonNode componentNode, String metricPath) {
        try {
            JsonNode componentInfo = componentNode.get("ServiceComponentInfo");
            String componentName = componentInfo.get("component_name").asText();
            String componentState = componentInfo.get("state").asText();

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
            ambariMetrics.put(metricPath + HadoopConstants.METRIC_SEPARATOR + componentName + "|state", states.indexOf(componentState));
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
            Long cpuCount = hostInfo.get("cpu_count").asLong();
            Long totalMem = hostInfo.get("total_mem").asLong();

            ambariMetrics.put(metricPath + "state", states.indexOf(hostState));
            ambariMetrics.put(metricPath + "cpuCount", cpuCount);
            ambariMetrics.put(metricPath + "totalMemory", totalMem);

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

            ambariMetrics.put(metricPath + "state", states.indexOf(componentState));
        } catch (Exception e) {
            logger.error(e);
        }
    }
}

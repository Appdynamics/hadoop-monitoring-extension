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
import com.appdynamics.monitors.hadoop.parser.Parser;
import org.apache.log4j.Logger;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AmbariCommunicator {
    private String host, port, user, password;
    private Logger logger;
    private JSONParser parser = new JSONParser();
    private Parser xmlParser;
    private Map<String, Object> metrics;
    private ExecutorService executor;

    private static final String CLUSTER_FIELDS = "?fields=services,hosts";
    private static final String SERVICE_FIELDS = "?fields=ServiceInfo/state,components";
    private static final String HOST_FIELDS = "?fields=Hosts/host_state,metrics";
    private static final String COMPONENT_FIELDS = "?fields=ServiceComponentInfo/state,metrics";


    /**
     * Constructs a new AmbariCommunicator. Metrics can be collected by calling {@link #populate(java.util.Map)}
     * Only metrics that match the conditions in <code>xmlParser</code> are collected.
     *
     * @param host
     * @param port
     * @param user
     * @param password
     * @param logger
     * @param xmlParser XML parser for metric filtering
     */
    public AmbariCommunicator(String host, String port, String user, String password, Logger logger, Parser xmlParser) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.logger = logger;
        this.xmlParser = xmlParser;

        executor = Executors.newFixedThreadPool(xmlParser.getThreadLimit());
    }

    /**
     * Populates <code>metrics</code> Map with all numeric Ambari clusters metrics.
     *
     * @param metrics
     * @see #getClusterMetrics(java.io.Reader)
     */
    public void populate(Map<String, Object> metrics) {
        this.metrics = metrics;
        try {
            Reader response = (new Response("", "/api/v1/clusters")).call();

            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            try {
                List<Map> clusters = (ArrayList<Map>) json.get("items");

                CompletionService<Reader> threadPool = new ExecutorCompletionService<Reader>(executor);
                int count = 0;
                for (Map cluster : clusters) {
                    if (xmlParser.isIncludeCluster((String) ((Map) cluster.get("Clusters")).get("cluster_name"))) {
                        threadPool.submit(new Response(cluster.get("href") + CLUSTER_FIELDS, ""));
                        count++;
                    }
                }
                for (; count > 0; count--) {
                    getClusterMetrics(threadPool.take().get());
                }
            } catch (Exception e) {
                logger.error("Failed to parse cluster names: " + stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for cluster names: " + stackTraceToString(e));
        }
        executor.shutdown();
    }

    private class Response implements Callable<Reader> {
        private SimpleHttpClient httpClient;
        private String url;
        private String uriPath;

        /**
         * Creates a preemptive basic authentication GET request to <code>location</code>
         *
         * @param url
         * @throws Exception
         */
        public Response(String url, String uriPath) throws Exception {
            httpClient = SimpleHttpClient.builder(buildHttpClientArguments()).build();
            this.url = url;
            this.uriPath = uriPath;
        }

        /**
         * @return StringReader representation of http response body
         * @throws Exception
         */

        public Reader call() throws Exception {
            com.appdynamics.extensions.http.Response response = null;
            try {
                response = httpClient.target(url).path(uriPath).get();
                return new StringReader(response.string());
            } catch (Exception e) {
                throw e;
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }
    }

    private Map<String, String> buildHttpClientArguments() {
        Map<String, String> clientArgs = new HashMap<String, String>();
        clientArgs.put(TaskInputArgs.HOST, host);
        clientArgs.put(TaskInputArgs.PORT, String.valueOf(port));
        clientArgs.put(TaskInputArgs.USER, user);
        clientArgs.put(TaskInputArgs.PASSWORD, password);
        return clientArgs;
    }

    /**
     * Parses a JSON Reader object as cluster metrics and collect service and host metrics.
     *
     * @param response
     * @see #getServiceMetrics(java.io.Reader, String)
     * @see #getHostMetrics(java.io.Reader, String)
     */
    private void getClusterMetrics(Reader response) {
        try {
            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            try {
                String clusterName = (String) ((Map) json.get("Clusters")).get("cluster_name");
                List<Map> services = (ArrayList<Map>) json.get("services");
                List<Map> hosts = (ArrayList<Map>) json.get("hosts");

                CompletionService<Reader> threadPool = new ExecutorCompletionService<Reader>(executor);
                int count = 0;
                for (Map service : services) {
                    if (xmlParser.isIncludeService((String) ((Map) service.get("ServiceInfo")).get("service_name"))) {
                        threadPool.submit(new Response(service.get("href") + SERVICE_FIELDS, ""));
                        count++;
                    }
                }
                for (; count > 0; count--) {
                    getServiceMetrics(threadPool.take().get(), clusterName + "|services");
                }

                for (Map host : hosts) {
                    if (xmlParser.isIncludeHost((String) ((Map) host.get("Hosts")).get("host_name"))) {
                        threadPool.submit(new Response(host.get("href") + HOST_FIELDS, ""));
                        count++;
                    }
                }
                for (; count > 0; count--) {
                    getHostMetrics(threadPool.take().get(), clusterName + "|hosts");
                }
            } catch (Exception e) {
                logger.error("Failed to parse cluster metrics: " + stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for cluster metrics: " + stackTraceToString(e));
        }
    }

    /**
     * Parses a JSON Reader object as service metrics and collect service state plus service
     * component metrics. Prefixes metric name with <code>hierarchy</code>.
     *
     * @param response
     * @param hierarchy
     * @see #getComponentMetrics(java.io.Reader, String)
     */
    private void getServiceMetrics(Reader response, String hierarchy) {
        try {
            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            try {
                Map serviceInfo = (Map) json.get("ServiceInfo");
                String serviceName = (String) serviceInfo.get("service_name");
                String serviceState = (String) serviceInfo.get("state");

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
                metrics.put(hierarchy + "|" + serviceName + "|state", states.indexOf(serviceState));

                List<Map> components = (ArrayList<Map>) json.get("components");

                CompletionService<Reader> threadPool = new ExecutorCompletionService<Reader>(executor);
                int count = 0;
                for (Map component : components) {
                    if (xmlParser.isIncludeServiceComponent(serviceName,
                            (String) ((Map) component.get("ServiceComponentInfo")).get("component_name"))) {
                        threadPool.submit(new Response(component.get("href") + COMPONENT_FIELDS, ""));
                        count++;
                    }
                }
                for (; count > 0; count--) {
                    getComponentMetrics(threadPool.take().get(), hierarchy + "|" + serviceName);
                }
            } catch (Exception e) {
                logger.error("Failed to parse service metrics: " + stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for service metrics: " + stackTraceToString(e));
        }
    }

    /**
     * Parses a JSON Reader object as host metrics and collect host state plus host metrics.
     * Prefixes metric name with <code>hierarchy</code>.
     *
     * @param response
     * @param hierarchy
     * @see #getAllMetrics(java.util.Map, String)
     */
    private void getHostMetrics(Reader response, String hierarchy) {
        try {
            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            try {
                Map hostInfo = (Map) json.get("Hosts");
                String hostName = (String) hostInfo.get("host_name");
                String hostState = (String) hostInfo.get("host_state");

                List<String> states = new ArrayList<String>();
                states.add("INIT");
                states.add("WAITING_FOR_HOST_STATUS_UPDATES");
                states.add("HEALTHY");
                states.add("HEARTBEAT_LOST");
                states.add("UNHEALTHY");
                metrics.put(hierarchy + "|" + hostName + "|state", states.indexOf(hostState));

                Map hostMetrics = (Map) json.get("metrics");

                //remove non metric data
                if(hostMetrics != null) {
                    hostMetrics.remove("boottime");

                    Iterator<Map.Entry> iter = hostMetrics.entrySet().iterator();
                    while (iter.hasNext()) {
                        if (!xmlParser.isIncludeHostMetrics((String) iter.next().getKey())) {
                            iter.remove();
                        }
                    }

                    getAllMetrics(hostMetrics, hierarchy + "|" + hostName);
                }
            } catch (Exception e) {
                logger.error("Failed to parse host metrics: " + stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for host metrics: " + stackTraceToString(e));
        }
    }

    /**
     * Parses a JSON Reader object as component metrics and collect component state plus all
     * numeric metrics. Prefixes metric name with <code>hierarchy</code>.
     *
     * @param response
     * @param hierarchy
     * @see #getAllMetrics(java.util.Map, String)
     */
    private void getComponentMetrics(Reader response, String hierarchy) {
        try {
            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            try {
                Map componentInfo = (Map) json.get("ServiceComponentInfo");
                String componentName = (String) componentInfo.get("component_name");
                String componentState = (String) componentInfo.get("state");

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
                metrics.put(hierarchy + "|" + componentName + "|state", states.indexOf(componentState));

                Map componentMetrics = (Map) json.get("metrics");
                if (componentMetrics == null) {
                    //no metrics
                    return;
                }
                //remove non metric data
                componentMetrics.remove("boottime");

                Iterator<Map.Entry> iter = componentMetrics.entrySet().iterator();
                while (iter.hasNext()) {
                    if (!xmlParser.isIncludeComponentMetrics((String) iter.next().getKey())) {
                        iter.remove();
                    }
                }

                getAllMetrics(componentMetrics, hierarchy + "|" + componentName);

            } catch (Exception e) {
                logger.error("Failed to parse component metrics: " + stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for component metrics: " + stackTraceToString(e));
        }
    }

    /**
     * Recursively parse JSON Map and collect only numeric metrics. Prefix metric name
     * with <code>hierarchy</code>.
     *
     * @param json
     * @param hierarchy
     */
    private void getAllMetrics(Map<String, Object> json, String hierarchy) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (val instanceof Map) {
                getAllMetrics((Map) val, hierarchy + "|" + key);
            } else if (val instanceof Number) {
                if (key.startsWith("load_")) {   //convert all load factors to percentage integers
                    if (val instanceof Double) {
                        metrics.put(hierarchy + "|" + key, (Double) val * 100);
                    } else {   //if Ganglia is down, load metrics are Long
                        metrics.put(hierarchy + "|" + key, val);
                    }
                } else {
                    metrics.put(hierarchy + "|" + key, val);
                }
            }
        }
    }

    /**
     * @param e
     * @return String representation of output from <code>printStackTrace()</code>
     */
    private String stackTraceToString(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}

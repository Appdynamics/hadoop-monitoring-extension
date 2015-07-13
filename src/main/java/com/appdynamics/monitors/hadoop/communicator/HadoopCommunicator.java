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
import com.appdynamics.extensions.http.Response;
import com.appdynamics.extensions.http.SimpleHttpClient;
import com.appdynamics.monitors.hadoop.config.ResourceManagerConfig;
import com.appdynamics.monitors.hadoop.hadoopexception.ResourceManagerMonitorException;
import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HadoopCommunicator {
    private static final String CLUSTER_METRIC_PATH = "/ws/v1/cluster/metrics";
    private static final String CLUSTER_SCHEDULER_PATH = "/ws/v1/cluster/scheduler";
    private static final String CLUSTER_APPS_PATH = "/ws/v1/cluster/apps";
    private static final String CLUSTER_NODES_PATH = "/ws/v1/cluster/nodes";
    private Logger logger = Logger.getLogger(HadoopCommunicator.class);
    private JSONParser parser = new JSONParser();

    private Map<String, Object> metrics;
    private long aggrAppPeriod;
    private SimpleHttpClient httpClient;


    /**
     * Constructs a new HadoopCommunicator. Metrics can be collected by calling {@link #populate(java.util.Map)}
     * Only metrics that match the conditions in <code>xmlParser</code> are collected.
     */
    public HadoopCommunicator(ResourceManagerConfig resourceManagerConfig) {
        aggrAppPeriod = resourceManagerConfig.getAggregateAppPeriod();
        httpClient = SimpleHttpClient.builder(buildHttpClientArguments(resourceManagerConfig)).build();
    }

    /**
     * Populates <code>metrics</code> with cluster metrics, scheduler info, app metrics, and node metrics.
     *
     * @param metrics
     */
    public void populate(Map<String, Object> metrics) {
        this.metrics = metrics;
        getClusterMetrics();
        getClusterScheduler();
        getAggrApps();
        getApplicationMetrics();
        getClusterNodes();
    }

    private Map<String, String> buildHttpClientArguments(ResourceManagerConfig resourceManagerConfig) {
        Map<String, String> clientArgs = new HashMap<String, String>();
        clientArgs.put(TaskInputArgs.HOST, resourceManagerConfig.getHost());
        clientArgs.put(TaskInputArgs.PORT, String.valueOf(resourceManagerConfig.getPort()));
        clientArgs.put(TaskInputArgs.USER, resourceManagerConfig.getUsername());
        clientArgs.put(TaskInputArgs.PASSWORD, resourceManagerConfig.getPassword());
        return clientArgs;
    }

    private Reader getResponse(String location) throws ResourceManagerMonitorException {
        Response response = null;
        try {
            response = httpClient.target().path(location).get();
            return new StringReader(response.string());
        } catch (Exception e) {
            logger.error("Failed to get HTTP Response from " + location, e);
            throw new ResourceManagerMonitorException("Failed to get HTTP Response from " + location, e);
        } finally {
            if (response != null) {
                response.close();
            }
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * Populates <code>metrics</code> with all cluster metrics.
     */
    private void getClusterMetrics() {
        try {
            Reader response = getResponse(CLUSTER_METRIC_PATH);
            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            json = (Map<String, Object>) json.get("clusterMetrics");

            for (Map.Entry<String, Object> entry : json.entrySet()) {
                metrics.put("clusterMetrics|" + entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            logger.error("Failed to parse ClusterMetrics: ", e);
        }
    }

    /**
     * Populates <code>metrics</code> with all scheduler metrics. Scheduler can be either Capacity
     * Scheduler or Fifo Scheduler.
     */
    private void getClusterScheduler() {
        try {
            Reader response = getResponse(CLUSTER_SCHEDULER_PATH);

            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            json = (Map<String, Object>) json.get("scheduler");
            json = (Map<String, Object>) json.get("schedulerInfo");

            if (json.get("type").equals("capacityScheduler")) {
                String queueName = (String) json.get("queueName");

                metrics.putAll(getQueues((ArrayList) ((Map) json.get("queues")).get("queue"), "schedulerInfo|" + queueName));

                json.remove("type");
                json.remove("queueName");
                json.remove("queues");

                for (Map.Entry<String, Object> entry : json.entrySet()) {
                    metrics.put("schedulerInfo|" + queueName + "|" + entry.getKey(), entry.getValue());
                }
            } else {    //fifoScheduler
                if (json.get("qstate").equals("RUNNING")) {
                    metrics.put("schedulerInfo|qstate", "1");
                } else {
                    metrics.put("schedulerInfo|qstate", "0");
                }

                json.remove("type");
                json.remove("qstate");

                for (Map.Entry<String, Object> entry : json.entrySet()) {
                    metrics.put("schedulerInfo|" + entry.getKey(), entry.getValue());
                }
            }
        } catch (Exception e) {
            logger.error("Failed to parse ClusterScheduler: ", e);
        }
    }

    /**
     * Returns a metric Map with all metrics in <code>queue</code>. Metric names
     * are prefixed with <code>hierarchy</code>
     *
     * @param queue
     * @param hierarchy
     * @return Map of queue metrics
     */
    private Map<String, Object> getQueues(List queue, String hierarchy) {
        Map<String, Object> queueMap = new HashMap<String, Object>();

        for (Map<String, Object> item : (ArrayList<Map<String, Object>>) queue) {
            String queueName = (String) item.get("queueName");

            if (item.get("queues") != null) {
                List queueList = (ArrayList) ((Map) item.get("queues")).get("queue");
                Map<String, Object> childQueue = getQueues(queueList, hierarchy + "|" + queueName);
                queueMap.putAll(childQueue);
            }

            queueMap.putAll(getResourcesUsed((Map) item.get("resourcesUsed"), hierarchy + "|" + queueName));
            if (item.get("users") != null) {
                queueMap.putAll(getUsers((ArrayList) ((Map) item.get("users")).get("user"), hierarchy + "|" + queueName));
            }

            //remove all non numerical type attributes
            item.remove("queueName");
            item.remove("queues");
            item.remove("state");
            item.remove("usedResources");
            item.remove("type");
            item.remove("resourcesUsed");
            item.remove("users");

            for (Map.Entry<String, Object> entry : item.entrySet()) {
                queueMap.put(hierarchy + "|" + queueName + "|" + entry.getKey(), entry.getValue());
            }
        }
        return queueMap;
    }

    /**
     * Returns a metric Map with all metrics in <code>resources</code>. Metric names
     * are prefixed with <code>hierarchy</code>
     *
     * @param resources
     * @param hierarchy
     * @return Map of resourcesUsed metrics
     */
    private Map<String, Object> getResourcesUsed(Map<String, Object> resources, String hierarchy) {
        Map<String, Object> rtn = new HashMap<String, Object>();

        for (Map.Entry<String, Object> entry : resources.entrySet()) {
            rtn.put(hierarchy + "|resourcesUsed|" + entry.getKey(), entry.getValue());
        }
        return rtn;
    }

    /**
     * Returns a metric Map with all metrics in <code>users</code>. Metric names
     * are prefixed with <code>hierarchy</code>
     *
     * @param users
     * @param hierarchy
     * @return Map of user metrics
     */
    private Map<String, Object> getUsers(List users, String hierarchy) {
        Map<String, Object> rtn = new HashMap<String, Object>();

        for (Map<String, Object> user : (ArrayList<Map<String, Object>>) users) {
            String username = (String) user.get("username");

            rtn.putAll(getResourcesUsed((Map<String, Object>) user.get("resourcesUsed"), hierarchy + "|" + username));

            user.remove("resourcesUsed");
            user.remove("username");

            for (Map.Entry<String, Object> entry : user.entrySet()) {
                rtn.put(hierarchy + "|users|" + username + "|" + entry.getKey(), entry.getValue());
            }
        }
        return rtn;
    }

    /**
     * Populates <code>metrics</code> with aggregated app metrics from non-finished apps
     * and apps finished in the last <code>aggrAppPeriod</code> milliseconds.
     * Metrics include average app progress and app count of all states (NEW, SUBMITTED,
     * ACCEPTED, RUNNING, FINISHED, FAILED, KILLED).
     */
    private void getAggrApps() {
        try {
            Date time = new Date();
            long currentTime = time.getTime();

            List<String> appIds = new ArrayList<String>();

            int[] appStateCount = new int[AppState.values().length];
            double avgProgress = 0;

            List<Reader> responses = new ArrayList<Reader>();
            responses.add(getResponse(CLUSTER_APPS_PATH + "?finishedTimeBegin=" + (currentTime - aggrAppPeriod)));
            responses.add(getResponse(CLUSTER_APPS_PATH + "?finalStatus=UNDEFINED"));

            for (Reader response : responses) {
                Map<String, Object> json = (Map<String, Object>) parser.parse(response);
                try {
                    json = (Map<String, Object>) json.get("apps");
                    if (json == null) {
                        //skip for empty app query
                        continue;
                    }
                    List<Map> appList = (ArrayList<Map>) json.get("app");

                    for (Map<String, Object> app : appList) {

                        String appName = (String) app.get("name");

                        String appId = (String) app.get("id");
                        if (!appIds.contains(appId)) {
                            String appState = (String) app.get("state");

                            appStateCount[AppState.valueOf(appState).ordinal()]++;
                            avgProgress += (Double) app.get("progress");

                            appIds.add(appId);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse aggregated apps: ", e);
                }
            }

            avgProgress = avgProgress / appIds.size();

            metrics.put("AggregatedApps|Average Progress", avgProgress);
            metrics.put("AggregatedApps|New Apps", appStateCount[AppState.NEW.ordinal()]);
            metrics.put("AggregatedApps|Submitted Apps", appStateCount[AppState.SUBMITTED.ordinal()]);
            metrics.put("AggregatedApps|Accepted Apps", appStateCount[AppState.ACCEPTED.ordinal()]);
            metrics.put("AggregatedApps|Running Apps", appStateCount[AppState.RUNNING.ordinal()]);
            metrics.put("AggregatedApps|Finished Apps", appStateCount[AppState.FINISHED.ordinal()]);
            metrics.put("AggregatedApps|Failed Apps", appStateCount[AppState.FAILED.ordinal()]);
            metrics.put("AggregatedApps|Killed Apps", appStateCount[AppState.KILLED.ordinal()]);

        } catch (Exception e) {
            logger.error("Failed to get response for aggregated apps: ", e);
        }
    }

    private void getApplicationMetrics() {
        try {
            Reader response = getResponse(CLUSTER_APPS_PATH);
            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            json = (Map<String, Object>) json.get("apps");
            if (json != null) {
                List<Map> appList = (ArrayList<Map>) json.get("app");

                for (Map<String, Object> app : appList) {

                    String appName = (String) app.get("name");
                    String metricPath = "Apps|" + appName + "|";
                    if (app.get("finishedTime") != null) {
                        metrics.put(metricPath + "finishedTime", app.get("finishedTime"));
                    }
                    metrics.put(metricPath + "progress", app.get("progress"));
                    metrics.put(metricPath + "startedTime", app.get("startedTime"));
                    if (app.get("elapsedTime") != null) {
                        metrics.put(metricPath + "ElapsedTime", app.get("elapsedTime"));
                    }
                    metrics.put(metricPath + "allocatedMB", app.get("allocatedMB"));
                    metrics.put(metricPath + "allocatedVCores", app.get("allocatedVCores"));
                    metrics.put(metricPath + "runningContainers", app.get("runningContainers"));
                    metrics.put(metricPath + "memorySeconds", app.get("memorySeconds"));
                    metrics.put(metricPath + "vcoreSeconds", app.get("vcoreSeconds"));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to parse response for apps: ", e);
        }
    }

    /**
     * Populates <code>metrics</code> with metrics from all nodes.
     */
    private void getClusterNodes() {
        try {
            Reader response = getResponse(CLUSTER_NODES_PATH);

            Map<String, Object> json = (Map<String, Object>) parser.parse(response);
            json = (Map<String, Object>) json.get("nodes");
            List<Map> nodeList = (ArrayList<Map>) json.get("node");

            for (Map<String, Object> node : nodeList) {
                metrics.putAll(getNode(node, "Nodes"));
            }
        } catch (Exception e) {
            logger.error("Failed to parse response for ClusterNodes: ", e);
        }
    }

    /**
     * Returns a metric Map with all node metrics in <code>node</code>. Metric names
     * are prefixed with <code>hierarchy</code>
     *
     * @param node
     * @param hierarchy
     * @return Map of node metrics
     */
    private Map<String, Object> getNode(Map<String, Object> node, String hierarchy) {
        Map<String, Object> rtn = new HashMap<String, Object>();

        String id = (String) node.get("id");

        if (node.get("healthStatus") != null && node.get("healthStatus").equals("Healthy")) {
            rtn.put(hierarchy + "|" + id + "|healthStatus", "1");
        } else {
            rtn.put(hierarchy + "|" + id + "|healthStatus", "0");
        }

        List<String> states = new ArrayList<String>();
        states.add("NEW");
        states.add("RUNNING");
        states.add("UNHEALTHY");
        states.add("DECOMMISSIONED");
        states.add("LOST");
        states.add("REBOOTED");
        rtn.put(hierarchy + "|" + id + "|state", states.indexOf(node.get("state")));

        rtn.put(hierarchy + "|" + id + "|usedMemoryMB", node.get("usedMemoryMB"));
        rtn.put(hierarchy + "|" + id + "|availMemoryMB", node.get("availMemoryMB"));
        rtn.put(hierarchy + "|" + id + "|numContainers", node.get("numContainers"));
        return rtn;
    }

    private enum AppState {
        NEW, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED
    }
}

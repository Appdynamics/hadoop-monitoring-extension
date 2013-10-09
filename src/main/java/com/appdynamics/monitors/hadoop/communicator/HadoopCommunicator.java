package com.appdynamics.monitors.hadoop.communicator;

import com.appdynamics.monitors.hadoop.parser.Parser;
import com.singularity.ee.util.httpclient.*;
import com.singularity.ee.util.log4j.Log4JLogger;
import org.apache.log4j.Logger;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HadoopCommunicator {
    private String baseAddress;
    private Logger logger;
    private JSONParser parser = new JSONParser();
    private Parser xmlParser;
    private NumberFormat numberFormat = NumberFormat.getInstance();


    private static final String CLUSTER_METRIC_PATH = "/ws/v1/cluster/metrics";
    private static final String CLUSTER_SCHEDULER_PATH = "/ws/v1/cluster/scheduler";
    private static final String CLUSTER_APPS_PATH = "/ws/v1/cluster/apps";
    private static final String CLUSTER_NODES_PATH = "/ws/v1/cluster/nodes";

    private ContainerFactory simpleContainer = new ContainerFactory() {
        @Override
        public Map createObjectContainer() {
            return new HashMap();
        }

        @Override
        public List creatArrayContainer() {
            return new ArrayList();
        }
    };

    /**
     * Constructs a new HadoopCommunicator. Metrics can be collected by calling {@link #populate(java.util.Map)}
     * Only metrics that match the conditions in <code>xmlParser</code> are collected.
     *
     * @param host
     * @param port
     * @param logger
     * @param xmlParser
     */
    public HadoopCommunicator(String host, String port, Logger logger, Parser xmlParser) {
        this.logger = logger;
        this.xmlParser = xmlParser;
        baseAddress = "http://" + host + ":" + port;
        numberFormat.setGroupingUsed(false);
    }

    /**
     * Populates <code>metrics</code> with cluster metrics, scheduler info, app metrics, and node metrics.
     *
     * @param metrics
     */
    public void populate(Map<String, String> metrics) {
        getClusterMetrics(metrics);
        getClusterScheduler(metrics);
        getClusterApps(metrics);
        getClusterNodes(metrics);
    }

    /**
     * Sends a http GET request to <code>location</code> and return the Reader representation of the
     * response body.
     *
     * @param location
     * @return Reader representation of the response body
     * @throws Exception
     */
    private Reader getResponse(String location) throws Exception {
        IHttpClientWrapper httpClient = HttpClientWrapper.getInstance();
        HttpExecutionRequest request = new HttpExecutionRequest(baseAddress + location,"", HttpOperation.GET);
        HttpExecutionResponse response = httpClient.executeHttpOperation(request,new Log4JLogger(logger));
        return new StringReader(response.getResponseBody());
    }

    /**
     * Populates <code>metrics</code> with all cluster metrics.
     *
     * @param metrics
     */
    private void getClusterMetrics(Map<String, String> metrics) {
        try {
            Reader response = getResponse(CLUSTER_METRIC_PATH);

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                json = (Map<String, Object>) json.get("clusterMetrics");

                for (Map.Entry<String, Object> entry : json.entrySet()){
                    metrics.put("clusterMetrics|" + entry.getKey(), entry.getValue().toString());
                }
            } catch (Exception e) {
                logger.error("Failed to parse ClusterMetrics: "+stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for ClusterMetrics: "+stackTraceToString(e));
        }
    }

    /**
     * Populates <code>metrics</code> with all scheduler metrics. Scheduler can be either Capacity
     * Scheduler or Fifo Scheduler.
     *
     * @param metrics
     */
    private void getClusterScheduler(Map<String, String> metrics) {
        try {
            Reader response = getResponse(CLUSTER_SCHEDULER_PATH);

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                json = (Map<String, Object>) json.get("scheduler");
                json = (Map<String, Object>) json.get("schedulerInfo");

                if (json.get("type").equals("capacityScheduler")){
                    String queueName = (String) json.get("queueName");

                    metrics.putAll(getQueues((ArrayList) ((Map) json.get("queues")).get("queue"), "schedulerInfo|" + queueName));

                    json.remove("type");
                    json.remove("queueName");
                    json.remove("queues");

                    for (Map.Entry<String, Object> entry : json.entrySet()){
                        metrics.put("schedulerInfo|" + queueName + "|" + entry.getKey(),
                                roundDecimal((Number) entry.getValue()));
                    }
                } else {    //fifoScheduler
                    if (json.get("qstate").equals("RUNNING")){
                        metrics.put("schedulerInfo|qstate", "1");
                    } else{
                        metrics.put("schedulerInfo|qstate", "0");
                    }

                    json.remove("type");
                    json.remove("qstate");

                    for (Map.Entry<String, Object> entry : json.entrySet()){
                        metrics.put("schedulerInfo|" + entry.getKey(), roundDecimal((Number) entry.getValue()));
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to parse ClusterScheduler: " + stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for ClusterScheduler: " + stackTraceToString(e));
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
    private Map<String, String> getQueues(List queue, String hierarchy){
        Map<String, String> queueMap = new HashMap<String, String>();

        for (Map<String, Object> item : (ArrayList<Map<String, Object>>) queue){
            String queueName = (String) item.get("queueName");

            if (item.get("queues") != null){
                List queueList = (ArrayList) ((Map) item.get("queues")).get("queue");
                Map<String, String> childQueue = getQueues(queueList, hierarchy + "|" + queueName);
                queueMap.putAll(childQueue);
            }

            queueMap.putAll(getResourcesUsed((Map) item.get("resourcesUsed"), hierarchy + "|" + queueName));
            if (item.get("users") != null){
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

            for (Map.Entry<String, Object> entry : item.entrySet()){
                queueMap.put(hierarchy + "|" + queueName + "|" + entry.getKey(),
                        roundDecimal((Number) entry.getValue()));
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
    private Map<String, String> getResourcesUsed(Map<String, Object> resources, String hierarchy){
        Map<String, String> rtn = new HashMap<String, String>();

        for (Map.Entry<String, Object> entry : resources.entrySet()){
            rtn.put(hierarchy + "|resourcesUsed|" + entry.getKey(), entry.getValue().toString());
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
    private Map<String, String> getUsers(List users, String hierarchy){
        Map<String, String> rtn = new HashMap<String, String>();

        for (Map<String, Object> user : (ArrayList<Map<String, Object>>) users){
            String username = (String) user.get("username");

            rtn.putAll(getResourcesUsed((Map<String, Object>) user.get("resourcesUsed"), hierarchy + "|" + username));

            user.remove("resourcesUsed");
            user.remove("username");

            for (Map.Entry<String, Object> entry : user.entrySet()){
                rtn.put(hierarchy + "|users|" + username + "|" + entry.getKey(), entry.getValue().toString());
            }
        }
        return rtn;
    }

    /**
     * Populates <code>metrics</code> with metrics from all apps.
     *
     * @param metrics
     */
    private void getClusterApps(Map<String, String> metrics) {
        try {
            Reader response = getResponse(CLUSTER_APPS_PATH);

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                json = (Map<String, Object>) json.get("apps");
                List<Map> appList = (ArrayList<Map>) json.get("app");

                for (Map<String, Object> app : appList){
                    metrics.putAll(getApp(app, "Apps"));
                }
            } catch (Exception e) {
                logger.error("Failed to parse ClusterApps: "+stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for ClusterApps: "+stackTraceToString(e));
        }
    }

    /**
     * Populate <code>metrics</code> with metrics from all nodes.
     *
     * @param metrics
     */
    private void getClusterNodes(Map<String, String> metrics) {
        try {
            Reader response = getResponse(CLUSTER_NODES_PATH);

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                json = (Map<String, Object>) json.get("nodes");
                List<Map> nodeList = (ArrayList<Map>) json.get("node");

                for (Map<String, Object> node : nodeList){
                    metrics.putAll(getNode(node, "Nodes"));
                }
            } catch (Exception e) {
                logger.error("Failed to parse ClusterNodes: "+stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for ClusterNodes: "+stackTraceToString(e));
        }
    }

    /**
     * Returns a metric Map with all app metrics in <code>app</code>. Metric names
     * are prefixed with <code>hierarchy</code>
     *
     * @param app
     * @param hierarchy
     * @return Map of app metrics
     */
    private Map<String, String> getApp(Map<String,Object> app, String hierarchy) {
        Map<String, String> rtn = new HashMap<String, String>();

        String appName = (String) app.get("name");
        if (!xmlParser.isIncludeAppName(appName)){
            return rtn;
        } else if (!xmlParser.isIncludeAppid((String) app.get("id"))){
            return rtn;
        }

        List<String> states = new ArrayList<String>();
        states.add("NEW");
        states.add("SUBMITTED");
        states.add("ACCEPTED");
        states.add("RUNNING");
        states.add("FINISHED");
        states.add("FAILED");
        states.add("KILLED");
        rtn.put(hierarchy + "|" + appName + "|state", String.valueOf(states.indexOf(app.get("state"))));

        List<String> finalStatus = new ArrayList<String>();
        finalStatus.add("UNDEFINED");
        finalStatus.add("SUCCEEDED");
        finalStatus.add("FAILED");
        finalStatus.add("KILLED");
        rtn.put(hierarchy + "|" + appName + "|finalStatus", String.valueOf(finalStatus.indexOf(app.get("finalStatus"))));

        Long progress = Math.round((Double) app.get("progress"));
        rtn.put(hierarchy+"|"+appName+"|progress", progress.toString());
        return rtn;
    }

    /**
     * Returns a metric Map with all node metrics in <code>node</code>. Metric names
     * are prefixed with <code>hierarchy</code>
     *
     * @param node
     * @param hierarchy
     * @return Map of node metrics
     */
    private Map<String, String> getNode(Map<String,Object> node, String hierarchy) {
        Map<String, String> rtn = new HashMap<String, String>();

        String id = (String) node.get("id");
        if (!xmlParser.isIncludeNodeid(id)){
            return rtn;
        }

        if (node.get("healthStatus").equals("Healthy")){
            rtn.put(hierarchy+"|"+id+"|healthStatus", "1");
        } else {
            rtn.put(hierarchy+"|"+id+"|healthStatus", "0");
        }

        List<String> states = new ArrayList<String>();
        states.add("NEW");
        states.add("RUNNING");
        states.add("UNHEALTHY");
        states.add("DECOMMISSIONED");
        states.add("LOST");
        states.add("REBOOTED");
        rtn.put(hierarchy+"|"+id+"|state", String.valueOf(states.indexOf(node.get("state"))));

        rtn.put(hierarchy+"|"+id+"|usedMemoryMB", node.get("usedMemoryMB").toString());
        rtn.put(hierarchy+"|"+id+"|availMemoryMB", node.get("availMemoryMB").toString());
        rtn.put(hierarchy+"|"+id+"|numContainers", node.get("numContainers").toString());
        return rtn;
    }

    /**
     *
     * @param num
     * @return String representation of rounded <code>num</code> in integer format
     */
    private String roundDecimal(Number num){
        if (num instanceof Float || num instanceof Double){
            return numberFormat.format(Math.round((Double) num));
        }
        return num.toString();
    }

    /**
     *
     * @param e
     * @return String representation of output from <code>printStackTrace()</code>
     */
    private String stackTraceToString(Exception e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}

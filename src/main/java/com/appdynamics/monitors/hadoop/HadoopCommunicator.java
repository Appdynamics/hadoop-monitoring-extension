package com.appdynamics.monitors.hadoop;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

public class HadoopCommunicator {
    private String baseAddress;
    private Logger logger;
    private JSONParser parser = new JSONParser();
    private Parser xmlParser;

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

    public HadoopCommunicator(String host, String port, Logger logger, Parser xmlParser) {
        this.logger = logger;
        this.xmlParser = xmlParser;
        baseAddress = "http://" + host + ":" + port;
    }

    public void populate(Map<String, String> metrics) {
        getClusterMetrics(metrics);
        getClusterScheduler(metrics);
        getClusterApps(metrics);
        getclusterNodes(metrics);
    }

    private Reader getResponse(String location) throws Exception {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(baseAddress + location);
        CloseableHttpResponse response = client.execute(httpGet);

        return new InputStreamReader(response.getEntity().getContent());
    }

    private void getClusterMetrics(Map<String, String> metrics) {
        try {
            Reader response = getResponse("/ws/v1/cluster/metrics");

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                json = (Map<String, Object>) json.get("clusterMetrics");

                for (Map.Entry<String, Object> entry : json.entrySet()){
                    metrics.put("clusterMetrics|" + entry.getKey(), entry.getValue().toString());
                }
            } catch (Exception e) {
                logger.error("Error: clusterMetrics empty"+json);
                logger.error("cluster err "+e);
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void getClusterScheduler(Map<String, String> metrics) {
        try {
            Reader response = getResponse("/ws/v1/cluster/scheduler");

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                json = (Map<String, Object>) json.get("scheduler");
                json = (Map<String, Object>) json.get("schedulerInfo");

                if (json.get("type").equals("capacityScheduler")){
                    ArrayList schedulerInfoList = new ArrayList();
                    schedulerInfoList.add(json);
                    metrics.putAll(getQueues(schedulerInfoList, "schedulerInfo"));
                } else {    //fifoScheduler
                    if (json.get("qstate").equals("RUNNING")){
                        metrics.put("schedulerInfo|qstate", "1");
                    } else{
                        metrics.put("schedulerInfo|qstate", "0");
                    }

                    json.remove("type");
                    json.remove("qstate");

                    for (Map.Entry<String, Object> entry : json.entrySet()){
                        metrics.put("schedulerInfo|" + entry.getKey(), roundDecimal((Number) entry.getValue()).toString());
                    }
                }
            } catch (Exception e) {
                logger.error("Error: clusterMetrics empty"+json);
                logger.error("cluster err "+e);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        }
    }

    private Map<String, String> getQueues(ArrayList queue, String hierarchy){
        Map<String, String> queueMap = new HashMap<String, String>();

        for (Map<String, Object> item : (ArrayList<Map>) queue){
            String queueName = (String) item.get("queueName");

            if (item.get("queues") != null){
                ArrayList queueList = (ArrayList) ((Map) item.get("queues")).get("queue");
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
                        roundDecimal((Number) entry.getValue()).toString());
            }
        }
        return queueMap;
    }

    private Map<String, String> getResourcesUsed(Map<String, Object> resources, String hierarchy){
        Map<String, String> rtn = new HashMap<String, String>();

        for (Map.Entry<String, Object> entry : resources.entrySet()){
            rtn.put(hierarchy + "|resourcesUsed|" + entry.getKey(), entry.getValue().toString());
        }
        return rtn;
    }

    private Map<String, String> getUsers(ArrayList users, String hierarchy){
        Map<String, String> rtn = new HashMap<String, String>();

        for (Map<String, Object> user : (ArrayList<Map>)users){
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

    private void getClusterApps(Map<String, String> metrics) {
        try {
            Reader response = getResponse("/ws/v1/cluster/apps");

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                json = (Map<String, Object>) json.get("apps");
                ArrayList<Map> appList = (ArrayList<Map>) json.get("app");

                for (Map<String, Object> app : appList){
                    metrics.putAll(getApp(app, "Apps"));
                }
            } catch (Exception e) {
                logger.error("cluster err "+e);
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void getclusterNodes(Map<String, String> metrics) {
        try {
            Reader response = getResponse("/ws/v1/cluster/nodes");

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                json = (Map<String, Object>) json.get("nodes");
                ArrayList<Map> nodeList = (ArrayList<Map>) json.get("node");

                for (Map<String, Object> node : nodeList){
                    metrics.putAll(getNode(node, "Nodes"));
                }
            } catch (Exception e) {
                logger.error("node err "+e);
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    //TODO: add state info as ints
    private Map<String, String> getApp(Map<String,Object> app, String hierarchy) {
        Map<String, String> rtn = new HashMap<String, String>();

        String appName = (String) app.get("name");
        if (xmlParser.getExcludedAppName().contains(appName)){
            return rtn;
        } else if (xmlParser.getExcludedAppid().contains(app.get("id"))){
            return rtn;
        }

        ArrayList<String> states = new ArrayList<String>();
        states.add("NEW");
        states.add("SUBMITTED");
        states.add("ACCEPTED");
        states.add("RUNNING");
        states.add("FINISHED");
        states.add("FAILED");
        states.add("KILLED");
        rtn.put(hierarchy + "|" + appName + "|state", String.valueOf(states.indexOf(app.get("state"))));

        ArrayList<String> finalStatus = new ArrayList<String>();
        finalStatus.add("UNDEFINED");
        finalStatus.add("SUCCEEDED");
        finalStatus.add("FAILED");
        finalStatus.add("KILLED");
        rtn.put(hierarchy + "|" + appName + "|finalStatus", String.valueOf(finalStatus.indexOf(app.get("finalStatus"))));

        Long progress = Math.round((Double) app.get("progress"));
        rtn.put(hierarchy+"|"+appName+"|progress", progress.toString());
        return rtn;
    }

    private Map<String, String> getNode(Map<String,Object> node, String hierarchy) {
        Map<String, String> rtn = new HashMap<String, String>();

        String id = (String) node.get("id");
        if (xmlParser.getExcludedNodeid().contains(id)){
            return rtn;
        }

        //should this be standardized?
        if (node.get("healthStatus").equals("Healthy")){
            rtn.put(hierarchy+"|"+id+"|healthStatus", "1");
        } else {
            rtn.put(hierarchy+"|"+id+"|healthStatus", "0");
        }

        ArrayList<String> states = new ArrayList<String>();
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

    //TODO: fix up type casting format so they're consistent
    //add pid appid for include metrics? or rather app_attempts since apps and nodes would have those?
    //exclude on pid, appid
    //exclude on specific metric path?(clustermetric|xxx)
    //
    //exclude by: appid, app name, node id
    //TODO: patch up with proper exception handling

    private Number roundDecimal(Number num){
        if (num.getClass() == Float.class || num.getClass() == Double.class){
            return Math.round((Double) num);
        }
        return num;
    }

}

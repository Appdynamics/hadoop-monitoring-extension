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

/**
 * Created with IntelliJ IDEA.
 * User: Stephen.Dong
 * Date: 9/14/13
 * Time: 4:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class HadoopCommunicator {
    private String baseAddress;
    private Logger logger;
    private JSONParser parser = new JSONParser();

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

    public HadoopCommunicator(String host, String port, Logger logger) {
        this.logger = logger;
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

        Reader reader = new InputStreamReader(response.getEntity().getContent());
        return reader;
    }

    private void getClusterMetrics(Map<String, String> metrics) {
        try {
            Reader response = getResponse("/ws/v1/cluster/metrics");

            Map json = (Map) parser.parse(response, simpleContainer);
            try {
                Map clusterMetrics = (Map) json.get("clusterMetrics");
                Iterator iter = clusterMetrics.entrySet().iterator();

                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();

                    metrics.put("clusterMetrics|" + entry.getKey(), String.valueOf(entry.getValue()));
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

            Map json = (Map) parser.parse(response, simpleContainer);
            try {
                Map<String, Object> scheduler = (Map<String, Object>) json.get("scheduler");
                scheduler = (Map<String, Object>) scheduler.get("schedulerInfo");

                if (scheduler.get("type").equals("capacityScheduler")){
                    ArrayList schedulerInfoList = new ArrayList();
                    schedulerInfoList.add(scheduler);

                    metrics.putAll(getQueue(schedulerInfoList, "schedulerInfo"));
                } else if (scheduler.get("type").equals("fifoScheduler")){
                    if (scheduler.get("qstate").equals("RUNNING")){
                        metrics.put("schedulerInfo|qstate", "1");
                    } else{
                        metrics.put("schedulerInfo|qstate", "0");
                    }
                    scheduler.remove("type");
                    scheduler.remove("qstate");

                    for (Map.Entry<String, Object> entry : scheduler.entrySet()){
                        Object val = entry.getValue();
                        if (val.getClass() == Float.class || val.getClass() == Double.class){
                            val = Math.round((Double) val);
                        }

                        metrics.put("schedulerInfo|" + entry.getKey(), val.toString());
                    }
                } else {
                    logger.error("type != expected values. ->"+scheduler.get("type"));
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

    private Map<String, String> getQueue(ArrayList queue, String hierarchy){
        Map<String, String> queueMap = new HashMap<String, String>();

        for (Map<String, Object> item : (ArrayList<Map>) queue){
            String queueName = (String) item.get("queueName");

            if (item.get("queues") != null){
                ArrayList queueList = (ArrayList) ((Map) item.get("queues")).get("queue");
                Map childQueue = getQueue(queueList, hierarchy+"|"+queueName);
                queueMap.putAll(childQueue);
            } else{
                System.out.println("current queue is a leaf queue:" + hierarchy+"|"+queueName);
            }

            //remove all non numerical type attributes
            item.remove("queueName");
            item.remove("queues");
            item.remove("state");
            item.remove("usedResources");
            item.remove("type");

            for (Map.Entry entry : item.entrySet()){
                String key = (String) entry.getKey();
                Object val = entry.getValue();

                if (key.equals("resourcesUsed")){
                    queueMap.putAll(getResourcesUsed((Map) val, hierarchy + "|" + queueName));
                } else if (key.equals("users")) {
                    if (val != null){
                        queueMap.putAll(getUsers((ArrayList)((Map) val).get("user"), hierarchy + "|" + queueName));
                    }
                } else {
                    if (val.getClass() == Float.class || val.getClass() == Double.class){
                        val = Math.round((Double) val);
                    }

                    queueMap.put(hierarchy + "|" + queueName + "|" + key, val.toString());
                }
            }
        }
        return queueMap;
    }

    private Map<String, String> getResourcesUsed(Map resources, String hierarchy){
        Map<String, String> rtn = new HashMap<String, String>();

        for (Map.Entry<String, Object> entry : ((Map<String, Object>) resources).entrySet()){
            rtn.put(hierarchy + "|resourcesUsed|" + entry.getKey(), entry.getValue().toString());
        }
        return rtn;
    }

    private Map<String, String> getUsers(ArrayList users, String hierarchy){
        Map<String, String> rtn = new HashMap<String, String>();

        for (Map<String, Object> user : (ArrayList<Map>)users){
            String username = (String) user.get("username");

            rtn.putAll(getResourcesUsed((Map) user.get("resourcesUsed"), hierarchy + "|" + username));
            user.remove("resourcesUsed");
            for (Map.Entry<String, Object> entry : user.entrySet()){
                rtn.put(hierarchy + "|users|" + username + "|" + entry.getKey(), entry.getValue().toString());
            }
        }
        return rtn;
    }

    private void getClusterApps(Map<String, String> metrics) {
        try {
            Reader response = getResponse("/ws/v1/cluster/apps");

            Map json = (Map) parser.parse(response, simpleContainer);
            try {
                Map<String, Object> apps = (Map<String, Object>) json.get("apps");
                ArrayList<Map> appList = (ArrayList) apps.get("app");

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

            Map json = (Map) parser.parse(response, simpleContainer);
            try {
                Map<String, Object> nodes = (Map<String, Object>) json.get("nodes");
                ArrayList<Map> nodeList = (ArrayList) nodes.get("node");

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

    private Map<String, String> getApp(Map<String,Object> app, String hierarchy) {
        Map<String, String> rtn = new HashMap<String, String>();
        //app doesn't seem to have any usable metrics except progress%

        String appName = (String) app.get("name");
        Long progress = Math.round((Double) app.get("progress"));
        rtn.put(hierarchy+"|"+appName+"|progress", progress.toString());

        return rtn;
    }

    private Map<String, String> getNode(Map<String,Object> node, String hierarchy) {
        Map<String, String> rtn = new HashMap<String, String>();

        String id = (String) node.get("id");
        rtn.put(hierarchy+"|"+id+"|usedMemoryMB", node.get("usedMemoryMB").toString());
        rtn.put(hierarchy+"|"+id+"|availMemoryMB", node.get("availMemoryMB").toString());
        rtn.put(hierarchy+"|"+id+"|numContainers", node.get("numContainers").toString());

        return rtn;
    }

    //TODO: fix up type casting format so they're consistent
}

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
import java.io.StringReader;
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
    }

    private Reader getResponse(String location) throws Exception {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(baseAddress + location);
        logger.info("GET "+baseAddress+location);
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
//            Reader response = getResponse("/ws/v1/cluster/scheduler");
            String test = "{\"scheduler\":{\"schedulerInfo\":{\"type\":\"capacityScheduler\",\"capacity\":100.0,\"usedCapacity\":0.0,\"maxCapacity\":100.0,\"queueName\":\"root\",\"queues\":{\"queue\":[{\"type\":\"capacitySchedulerLeafQueueInfo\",\"capacity\":100.0,\"usedCapacity\":0.0,\"maxCapacity\":100.0,\"absoluteCapacity\":100.0,\"absoluteMaxCapacity\":100.0,\"absoluteUsedCapacity\":0.0,\"numApplications\":0,\"usedResources\":\"memory: 0\",\"queueName\":\"default\",\"state\":\"RUNNING\",\"resourcesUsed\":{\"memory\":0},\"numActiveApplications\":0,\"numPendingApplications\":0,\"numContainers\":0,\"maxApplications\":10000,\"maxApplicationsPerUser\":10000,\"maxActiveApplications\":1,\"maxActiveApplicationsPerUser\":1,\"userLimit\":100,\"users\":null,\"userLimitFactor\":1.0}]}}}}";
            Reader response = new StringReader(test);

            Map json = (Map) parser.parse(response, simpleContainer);
            try {
                Map scheduler = (Map) json.get("scheduler");
                Iterator iter = scheduler.entrySet().iterator();

                //TODO: flatten scheduler
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    //round float/double to long
                    Long val = Math.round((Double) entry.getValue());

                    metrics.put("scheduler|" + entry.getKey(), val.toString());
                }
            } catch (Exception e) {
                logger.error("Error: clusterMetrics empty"+json);
                logger.error("cluster err "+e);
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    //TODO: recursive function to flatten scheduler queue
    private Map<String, String> getQueue(ArrayList queue, String hierarchy){
        Map<String, String> queueMap = new HashMap<String, String>();

        for (Map<String, Object> item : (ArrayList<Map>) queue){
            String queueName = (String) item.get("queueName");
            ArrayList queueList = (ArrayList) ((Map) item.get("queues")).get("queue");
            Map childQueue = getQueue(queueList, hierarchy+"|"+queueName);
            queueMap.putAll(childQueue);

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
                } else if (key.equals("users") && val != null) {
                    queueMap.putAll(getUsers((ArrayList)((Map) val).get("user"), hierarchy + "|" + queueName));
                } else {
                    queueMap.put(hierarchy + "|" + queueName + "|" + key,
                            String.valueOf(entry.getValue()));
                }
            }
        }
        return queueMap;
    }

    private Map<String, String> getResourcesUsed(Map resources, String hierarchy){
        Map<String, String> rtn = new HashMap<String, String>();

        rtn.put(hierarchy + "|resourcesUsed|memory", resources.get("memory").toString());
        rtn.put(hierarchy + "|resourcesUsed|vCores", resources.get("vCores").toString());

        return rtn;
    }

    private Map<String, String> getUsers(ArrayList users, String hierarchy){
        Map<String, String> rtn = new HashMap<String, String>();

        for (Map<String, Object> user : (ArrayList<Map>)users){
            String username = (String) user.get("username");

            rtn.putAll(getResourcesUsed((Map) user.get("resourcesUsed"), hierarchy + "|" + username));
            rtn.put(hierarchy + "|users|" + username + "|numActiveApplications", user.get("numActiveApplications").toString());
            rtn.put(hierarchy + "|users|" + username + "|numPendingApplications", user.get("numPendingApplications").toString());
        }

        return rtn;
    }
}

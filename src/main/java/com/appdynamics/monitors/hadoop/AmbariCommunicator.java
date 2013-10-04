package com.appdynamics.monitors.hadoop;

import com.singularity.ee.util.httpclient.*;
import com.singularity.ee.util.log4j.Log4JLogger;
import org.apache.log4j.Logger;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import java.io.Reader;
import java.io.StringReader;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: stephen.dong
 * Date: 9/24/13
 * Time: 4:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class AmbariCommunicator {
    private String host, port, user, password;
    private Logger logger;
    private JSONParser parser = new JSONParser();
    private Parser xmlParser;
    private Map<String, String> metrics;
    private NumberFormat numberFormat = NumberFormat.getInstance();
    private ExecutorService executor;

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

    public AmbariCommunicator(String host, String port, String user, String password, Logger logger, Parser xmlParser){
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.logger = logger;
        this.xmlParser = xmlParser;

        numberFormat.setGroupingUsed(false);
        executor = Executors.newFixedThreadPool(xmlParser.getThreadLimit());
    }

    public void populate(Map<String, String> metrics) {
        this.metrics = metrics;
        try {
            Reader response = (new Response("http://" + host + ":" + port + "/api/v1/clusters")).call();

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                List<Map> clusters = (ArrayList<Map>) json.get("items");

                CompletionService<Reader> threadPool = new ExecutorCompletionService<Reader>(executor);
                int count = 0;
                for (Map cluster : clusters){
                    if (xmlParser.isIncludeCluster((String) ((Map) cluster.get("Clusters")).get("cluster_name"))){
                        threadPool.submit(new Response(cluster.get("href") + "?fields=services,hosts"));
                        count++;
                    }
                }
                for(;count>0;count--){
                    getClusterMetrics(threadPool.take().get());
                }
            } catch (Exception e) {
                logger.error("Failed to parse cluster names: " + e);
            }
        } catch (Exception e) {
            logger.error("Failed to get response for cluster names: " + e);
        }
        executor.shutdown();
    }

    private class Response implements Callable<Reader>{
        private IHttpClientWrapper httpClient;
        private HttpExecutionRequest request;

        public Response(String location) throws Exception{
            httpClient = HttpClientWrapper.getInstance();
            request = new HttpExecutionRequest(location,"", HttpOperation.GET);
            httpClient.authenticateHost(host, Integer.parseInt(port), "", user, password, true);
        }

        @Override
        public Reader call() throws Exception {
            HttpExecutionResponse response = httpClient.executeHttpOperation(request,new Log4JLogger(logger));
            return new StringReader(response.getResponseBody());
        }
    }

    private void getClusterMetrics(Reader response){
        try {
            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                String clusterName = (String) ((Map) json.get("Clusters")).get("cluster_name");
                List<Map> services = (ArrayList<Map>) json.get("services");
                List<Map> hosts = (ArrayList<Map>) json.get("hosts");

                CompletionService<Reader> threadPool = new ExecutorCompletionService<Reader>(executor);
                int count = 0;
                for (Map service : services){
                    if (xmlParser.isIncludeService((String) ((Map) service.get("ServiceInfo")).get("service_name"))){
                        threadPool.submit(new Response(service.get("href") + "?fields=ServiceInfo/state,components"));
                        count++;
                    }
                }
                for(;count>0;count--){
                    getServiceMetrics(threadPool.take().get(), clusterName + "|services");
                }

                for (Map host : hosts){
                    if (xmlParser.isIncludeHost((String) ((Map) host.get("Hosts")).get("Host_name"))){
                        threadPool.submit(new Response(host.get("href") + "?fields=Hosts/host_state,metrics"));
                        count++;
                    }
                }
                for(;count>0;count--){
                    getHostMetrics(threadPool.take().get(), clusterName + "|hosts");
                }
            } catch (Exception e) {
                logger.error("Failed to parse cluster metrics: " + e);
            }
        } catch (Exception e) {
            logger.error("Failed to get response for cluster metrics: " + e);
        }
    }

    private void getServiceMetrics(Reader response, String hierarchy){
        try {
            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
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
                metrics.put(hierarchy + "|" + serviceName + "|state", String.valueOf(states.indexOf(serviceState)));

                List<Map> components = (ArrayList<Map>) json.get("components");

                CompletionService<Reader> threadPool = new ExecutorCompletionService<Reader>(executor);
                int count = 0;
                for (Map component : components){
                    if (xmlParser.isIncludeServiceComponent(serviceName,
                            (String) ((Map) component.get("ServiceComponentInfo")).get("component_name"))){
                        threadPool.submit(new Response(component.get("href") + "?fields=ServiceComponentInfo/state,metrics"));
                        count++;
                    }
                }
                for(;count>0;count--){
                    getComponentMetrics(threadPool.take().get(), hierarchy + "|" + serviceName);
                }
            } catch (Exception e) {
                logger.error("Failed to parse service metrics: " + e);
            }
        } catch (Exception e) {
            logger.error("Failed to get response for service metrics: " + e);
        }
    }

    private void getHostMetrics(Reader response, String hierarchy){
        try {
            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
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
                metrics.put(hierarchy + "|" + hostName + "|state", String.valueOf(states.indexOf(hostState)));

                Map hostMetrics = (Map) json.get("metrics");

                //remove non metric data
                hostMetrics.remove("boottime");

                Iterator<Map.Entry> iter = hostMetrics.entrySet().iterator();
                while(iter.hasNext()){
                    if (!xmlParser.isIncludeHostMetrics((String) iter.next().getKey())){
                        iter.remove();
                    }
                }

                getAllMetrics(hostMetrics, hierarchy + "|" + hostName);
            } catch (Exception e) {
                logger.error("Failed to parse host metrics: " + e);
            }
        } catch (Exception e) {
            logger.error("Failed to get response for host metrics: " + e);
        }
    }

    private void getComponentMetrics(Reader response, String hierarchy){
        try {
            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
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
                metrics.put(hierarchy + "|" + componentName + "|state", String.valueOf(states.indexOf(componentState)));

                Map componentMetrics = (Map) json.get("metrics");
                if (componentMetrics == null){
                    //no metrics
                    return;
                }
                //remove non metric data
                componentMetrics.remove("boottime");

                Iterator<Map.Entry> iter = componentMetrics.entrySet().iterator();
                while(iter.hasNext()){
                    if (!xmlParser.isIncludeComponentMetrics((String) iter.next().getKey())){
                        iter.remove();
                    }
                }

                getAllMetrics(componentMetrics, hierarchy + "|" + componentName);
            } catch (Exception e) {
                logger.error("Failed to parse component metrics: " + e);
            }
        } catch (Exception e) {
            logger.error("Failed to get response for component metrics: " + e);
        }
    }

    //ignores all non numeric attributes and all lists
    //load % expressed as reals!
    //what other metrics are like this?
    private void getAllMetrics(Map<String, Object> json, String hierarchy) {
        for (Map.Entry<String, Object> entry : json.entrySet()){
            String key = entry.getKey();
            Object val = entry.getValue();
            if (val instanceof Map){
                getAllMetrics((Map) val, hierarchy + "|" + key);
            } else if (val instanceof Number){
                if (key.startsWith("load_")){
                    metrics.put(hierarchy + "|" + key, roundDecimal(((Double) val)*100));
                } else {
                    metrics.put(hierarchy + "|" + key, roundDecimal((Number) val));
                }
            }
        }
    }

    private String roundDecimal(Number num){
        if (num instanceof Float || num instanceof Double){
            return numberFormat.format(Math.round((Double) num));
        }
        return num.toString();
    }
}

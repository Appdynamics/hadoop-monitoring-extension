package com.appdynamics.monitors.hadoop;

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
import java.util.*;
import java.util.concurrent.*;

public class AmbariCommunicator {
    private String host, port, user, password;
    private Logger logger;
    private JSONParser parser = new JSONParser();
    private Parser xmlParser;
    private Map<String, String> metrics;
    private NumberFormat numberFormat = NumberFormat.getInstance();
    private ExecutorService executor;

    private static final String CLUSTER_FIELDS = "?fields=services,hosts";
    private static final String SERVICE_FIELDS = "?fields=ServiceInfo/state,components";
    private static final String HOST_FIELDS = "?fields=Hosts/host_state,metrics";
    private static final String COMPONENT_FIELDS = "?fields=ServiceComponentInfo/state,metrics";

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

    /**
     * Populate <code>metrics</code> Map with all numeric Ambari clusters metrics.
     * @see #getClusterMetrics(java.io.Reader)
     *
     * @param metrics
     */
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
                        threadPool.submit(new Response(cluster.get("href") + CLUSTER_FIELDS));
                        count++;
                    }
                }
                for(;count>0;count--){
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

    private class Response implements Callable<Reader>{
        private IHttpClientWrapper httpClient;
        private HttpExecutionRequest request;

        /**
         * Create a preemptive basic authentication GET request to <code>location</code>
         *
         * @param location
         * @throws Exception
         */
        public Response(String location) throws Exception{
            httpClient = HttpClientWrapper.getInstance();
            request = new HttpExecutionRequest(location,"", HttpOperation.GET);
            httpClient.authenticateHost(host, Integer.parseInt(port), "", user, password, true);
        }

        /**
         *
         * @return StringReader representation of http response body
         * @throws Exception
         */
        @Override
        public Reader call() throws Exception {
            HttpExecutionResponse response = httpClient.executeHttpOperation(request,new Log4JLogger(logger));
            return new StringReader(response.getResponseBody());
        }
    }

    /**
     * Parse a JSON Reader object as cluster metrics and collect service and host metrics.
     * @see #getServiceMetrics(java.io.Reader, String)
     * @see #getHostMetrics(java.io.Reader, String)
     *
     * @param response
     */
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
                        threadPool.submit(new Response(service.get("href") + SERVICE_FIELDS));
                        count++;
                    }
                }
                for(;count>0;count--){
                    getServiceMetrics(threadPool.take().get(), clusterName + "|services");
                }

                for (Map host : hosts){
                    if (xmlParser.isIncludeHost((String) ((Map) host.get("Hosts")).get("host_name"))){
                        threadPool.submit(new Response(host.get("href") + HOST_FIELDS));
                        count++;
                    }
                }
                for(;count>0;count--){
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
     * Parse a JSON Reader object as service metrics and collect service state plus service
     * component metrics. Prefix metric name with <code>hierarchy</code>.
     * @see #getComponentMetrics(java.io.Reader, String)
     *
     * @param response
     * @param hierarchy
     */
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
                        threadPool.submit(new Response(component.get("href") + COMPONENT_FIELDS));
                        count++;
                    }
                }
                for(;count>0;count--){
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
     * Parse a JSON Reader object as host metrics and collect host state plus host metrics.
     * Prefix metric name with <code>hierarchy</code>.
     * @see #getAllMetrics(java.util.Map, String)
     *
     * @param response
     * @param hierarchy
     */
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
                logger.error("Failed to parse host metrics: " + stackTraceToString(e));
            }
        } catch (Exception e) {
            logger.error("Failed to get response for host metrics: " + stackTraceToString(e));
        }
    }

    /**
     * Parse a JSON Reader object as component metrics and collect component state plus all
     * numeric metrics. Prefix metric name with <code>hierarchy</code>.
     * @see #getAllMetrics(java.util.Map, String)
     *
     * @param response
     * @param hierarchy
     */
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
        for (Map.Entry<String, Object> entry : json.entrySet()){
            String key = entry.getKey();
            Object val = entry.getValue();
            if (val instanceof Map){
                getAllMetrics((Map) val, hierarchy + "|" + key);
            } else if (val instanceof Number){
                if (key.startsWith("load_")){   //convert all load factors to integers
                    metrics.put(hierarchy + "|" + key, roundDecimal(((Double) val)*100));
                } else {
                    metrics.put(hierarchy + "|" + key, roundDecimal((Number) val));
                }
            }
        }
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

package com.appdynamics.monitors.hadoop;

import com.singularity.ee.util.httpclient.*;
import com.singularity.ee.util.log4j.Log4JLogger;
//import org.apache.http.Header;
//import org.apache.http.HttpRequest;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.UsernamePasswordCredentials;
//import org.apache.http.client.CredentialsProvider;
//import org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.auth.BasicScheme;
//import org.apache.http.impl.client.BasicCredentialsProvider;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
//import org.apache.http.message.BasicHeader;
//import org.apache.http.protocol.BasicHttpContext;
import org.apache.log4j.Logger;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

//import java.io.InputStreamReader;
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
    private String baseAddress, host, port;
    private Logger logger;
    private String user, password;
    private JSONParser parser = new JSONParser();
    private Parser xmlParser;
    private Map<String, String> metrics;
    private NumberFormat numberFormat = NumberFormat.getInstance();
    private ExecutorService executor;

    int acc = 0;
    int count = 0;

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
        baseAddress = "http://" + host + ":" + port + "/api/v1";

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
                logger.error("Error: clusterMetrics empty"+json);
                logger.error("cluster err " + e);
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        }
        executor.shutdown();
        logger.info("total requests: "+ count);
        logger.info("total response size: "+acc);
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
//                logger.error("href: "+href);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
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
//                logger.error("href: "+href);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
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
//                logger.error("href: "+href);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
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
//                logger.error("href: "+href);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        }
    }

    //ignores all non numeric attributes and all lists
    private void getAllMetrics(Map<String, Object> json, String hierarchy) {
        for (Map.Entry<String, Object> entry : json.entrySet()){
            String key = entry.getKey();
            Object val = entry.getValue();
            if (val instanceof Map){
                getAllMetrics((Map) val, hierarchy + "|" + key);
            } else if (val instanceof Number){
                metrics.put(hierarchy + "|" + key, roundDecimal((Number) val));
            }
        }
    }

    private String roundDecimal(Number num){
        if (num.getClass() == Float.class || num.getClass() == Double.class){
            return numberFormat.format(Math.round((Double) num));
        }
        return num.toString();
    }
}

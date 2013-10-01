package com.appdynamics.monitors.hadoop;

import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.log4j.Logger;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import java.io.InputStreamReader;
import java.io.Reader;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        executor = Executors.newFixedThreadPool(100);
    }

    public void populate(Map<String, String> metrics) {
        this.metrics = metrics;
        try {
            Reader response = (new Response("http://" + host + ":" + port + "/api/v1/clusters")).call();
//            Reader response = getResponse("http://" + host + ":" + port + "/api/v1/clusters");

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                List<Map> clusters = (ArrayList<Map>) json.get("items");
//                for (Map cluster : clusters){
//                    getClusterMetrics((String) cluster.get("href"), "");
//                }
                List<Future> responses = new ArrayList<Future>();
                for (Map cluster : clusters){
                    Future<Reader> readerFuture = executor.submit(
                            new Response(cluster.get("href") + "?fields=services,hosts"));
                    responses.add(readerFuture);
                }
                for (Future<Reader> httpResponse : responses){
                    getClusterMetrics(httpResponse.get());
                }
            } catch (Exception e) {
                logger.error("Error: clusterMetrics empty"+json);
                logger.error("cluster err "+e);
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        }
        executor.shutdown();
        logger.info("total requests: "+ count);
        logger.info("total response size: "+acc);
    }

//    private Reader getResponse(String location) throws Exception {
////        logger.info("Getting Response for " + location);
//        UsernamePasswordCredentials cred = new UsernamePasswordCredentials(user, password);
//        CloseableHttpClient httpClient = HttpClients.createDefault();
//        HttpGet httpGet = new HttpGet(location);
//        Header httpHeader = new BasicScheme().authenticate(cred, httpGet, new BasicHttpContext());
//        httpGet.addHeader(httpHeader);
//        CloseableHttpResponse response = httpClient.execute(httpGet);
//
//        count++;
//        if (response.getEntity().getContentLength()>0){
//            acc += response.getEntity().getContentLength();
//        } else {
//            logger.info(location + " content might be chunked");
//        }
//
//        return new InputStreamReader(response.getEntity().getContent());
//    }

    private class Response implements Callable<Reader>{
        private CloseableHttpClient httpClient;
        private HttpGet httpGet;

        public Response(String location) throws Exception{
            UsernamePasswordCredentials cred = new UsernamePasswordCredentials(user, password);
            httpClient = HttpClients.createDefault();
            httpGet = new HttpGet(location);
            Header httpHeader = new BasicScheme().authenticate(cred, httpGet, new BasicHttpContext());
            httpGet.addHeader(httpHeader);
        }

        @Override
        public Reader call() throws Exception {
            CloseableHttpResponse response = httpClient.execute(httpGet);
            return new InputStreamReader(response.getEntity().getContent());
        }
    }

    private void getClusterMetrics(Reader response){
        try {
//            Reader response = getResponse(href + "?fields=services,hosts");

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                String clusterName = (String) ((Map) json.get("Clusters")).get("cluster_name");
                List<Map> services = (ArrayList<Map>) json.get("services");
                List<Map> hosts = (ArrayList<Map>) json.get("hosts");
//                for (Map service : services){
//                    getServiceMetrics((String) service.get("href"), hierarchy + "|" + clusterName + "|services");
//                }
                List<Future> responses = new ArrayList<Future>();
                for (Map service : services){
                    Future<Reader> readerFuture = executor.submit(
                            new Response(service.get("href") + "?fields=ServiceInfo/state,components"));
                    responses.add(readerFuture);
                }
                for (Future<Reader> httpResponse : responses){
                    getServiceMetrics(httpResponse.get(), clusterName + "|services");
                }
                responses.clear();
//                for (Map host : hosts){
//                    //DO NOT get the entire json obj from hosts
//                    getHostMetrics((String) host.get("href"), hierarchy + "|" + clusterName + "|hosts");
//                }
                for (Map host : hosts){
                    Future<Reader> readerFuture = executor.submit(
                            new Response(host.get("href") + "?fields=Hosts/host_state,metrics"));
                    responses.add(readerFuture);
                }
                for (Future<Reader> httpResponse : responses){
                    getHostMetrics(httpResponse.get(), clusterName + "|hosts");
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

    //TODO: thread this
    private void getServiceMetrics(Reader response, String hierarchy){
        try {
//            Reader response = getResponse(href + "?fields=ServiceInfo/state,components");

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
                List<Future> responses = new ArrayList<Future>();
                for (Map component : components){
                    Future<Reader> readerFuture = executor.submit(
                            new Response((String) component.get("href") + "?fields=ServiceComponentInfo/state,metrics"));
                    responses.add(readerFuture);
                }
                for (Future<Reader> httpResponse : responses){
                    getComponentMetrics(httpResponse.get(), hierarchy + "|" + serviceName + "|components");
                }
//                for (Map component : components){
//                    getComponentMetrics((String) component.get("href"), hierarchy + "|" + serviceName + "|services");
//                }
            } catch (Exception e) {
//                logger.error("href: "+href);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        }
    }

    //TODO: thread this
    private void getHostMetrics(Reader response, String hierarchy){
        try {
//            Reader response = getResponse(href + "?fields=Hosts/host_state,metrics");

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
//                if (hostMetrics == null){
//                    //no metrics
//                    return;
//                }
                //remove non metric data
                hostMetrics.remove("boottime");

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

    //TODO: thread this
    private void getComponentMetrics(Reader response, String hierarchy){
        try {
//            Reader response = getResponse(href + "?fields=ServiceComponentInfo/state,metrics");

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

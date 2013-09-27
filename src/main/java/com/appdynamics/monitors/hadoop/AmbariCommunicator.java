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
    }

    public void populate(Map<String, String> metrics) {
        this.metrics = metrics;
        try {
            Reader response = getResponse("http://" + host + ":" + port + "/api/v1/clusters");

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                /*
                {
                  "href" : "http://ec2-23-22-170-13.compute-1.amazonaws.com:8080/api/v1/clusters",
                  "items" : [
                    {
                      "href" : "http://ec2-23-22-170-13.compute-1.amazonaws.com:8080/api/v1/clusters/testCluster",
                      "Clusters" : {
                        "cluster_name" : "testCluster",
                        "version" : "HDP-1.3.2"
                      }
                    }
                  ]
                }
                 */
                //there's a LOT of metrics, accessible by 'href' attr that leads to more detailed info
                List<Map> clusters = (ArrayList<Map>) json.get("items");
                for (Map cluster : clusters){
                    //TODO: get individual cluster metrics
                    getClusterMetrics((String) cluster.get("href"), "");
                }
            } catch (Exception e) {
                logger.error("Error: clusterMetrics empty"+json);
                logger.error("cluster err "+e);
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        }
    }

    private Reader getResponse(String location) throws Exception {
        UsernamePasswordCredentials cred = new UsernamePasswordCredentials(user, password);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(location);
        Header httpHeader = new BasicScheme().authenticate(cred, httpGet, new BasicHttpContext());
        httpGet.addHeader(httpHeader);
        CloseableHttpResponse response = httpClient.execute(httpGet);

        return new InputStreamReader(response.getEntity().getContent());
    }

    private void getClusterMetrics(String href, String hierarchy){
        try {
            Reader response = getResponse(href);

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                String clusterName = (String) ((Map) json.get("Clusters")).get("cluster_name");
                List<Map> services = (ArrayList<Map>) json.get("services");
                List<Map> hosts = (ArrayList<Map>) json.get("hosts");
                for (Map service : services){
                    getServiceMetrics((String) service.get("href"), hierarchy + "|" + clusterName + "|services");
                }
                for (Map host : hosts){
                    //DO NOT get the entire json obj from hosts
                    getHostMetrics((String) host.get("href"), hierarchy + "|" + clusterName + "|hosts");
                }
            } catch (Exception e) {
                logger.error("href: "+href);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void getServiceMetrics(String href, String hierarchy){
        try {
            Reader response = getResponse(href);

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
                for (Map component : components){
                    //TODO: get individual cluster metrics
                    getComponentMetrics((String) component.get("href"), hierarchy + "|" + serviceName + "|services");
                }
            } catch (Exception e) {
                logger.error("href: "+href);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void getHostMetrics(String href, String hierarchy){

    }

    private void getComponentMetrics(String href, String hierarchy){
        try {
            Reader response = getResponse(href);

            Map<String, Object> json = (Map<String, Object>) parser.parse(response, simpleContainer);
            try {
                Map componentInfo = (Map) json.get("ServiceComponentInfo");
                String componentName = (String) componentInfo.get("component_name");
                String componentState = (String) componentInfo.get("state");
//                logger.info("state: "+componentState);

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

//                logger.info("printing all metrics for "+href);
                getAllMetrics(componentMetrics, hierarchy + "|" + componentName);
            } catch (Exception e) {
                logger.error("href: "+href);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error(e);
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
            } else {
//                logger.info(hierarchy + "|" + key + " is a list: " +val.getClass().getName());
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

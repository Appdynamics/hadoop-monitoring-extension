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
}

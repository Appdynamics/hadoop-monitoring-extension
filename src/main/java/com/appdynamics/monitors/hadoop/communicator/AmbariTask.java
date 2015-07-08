package com.appdynamics.monitors.hadoop.communicator;

import com.appdynamics.extensions.http.Response;
import com.appdynamics.extensions.http.SimpleHttpClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * Created by balakrishnav on 25/6/15.
 */
public class AmbariTask implements Callable<JsonNode> {
    private Logger logger = Logger.getLogger(AmbariTask.class);
    private String url;
    private String uriPath;
    private SimpleHttpClient httpClient;

    public AmbariTask(SimpleHttpClient httpClient, String url, String uriPath) {
        this.httpClient = httpClient;
        this.url = url;
        this.uriPath = uriPath;
    }

    public JsonNode call() throws Exception {
        Response response = null;
        try {
            response = httpClient.target(url).path(uriPath).get();
            JsonNode node = new ObjectMapper().readValue(response.string(), JsonNode.class);
            return node;
        } catch (Exception e) {
            throw e;
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }
}

package com.appdynamics.monitors.hadoop.config;

/**
 * Created by balakrishnav on 24/6/15.
 */
public class ResourceManagerConfig {
    private String host;
    private int port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}

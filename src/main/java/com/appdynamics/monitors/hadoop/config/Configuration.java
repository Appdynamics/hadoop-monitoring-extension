package com.appdynamics.monitors.hadoop.config;

/**
 * Created by balakrishnav on 24/6/15.
 */
public class Configuration {
    private boolean resourceManagerMonitor;
    private ResourceManagerConfig resourceManagerConfig;
    private boolean ambariMonitor;
    private AmbariConfig ambariConfig;
    private String metricPathPrefix;

    public ResourceManagerConfig getResourceManagerConfig() {
        return resourceManagerConfig;
    }

    public void setResourceManagerConfig(ResourceManagerConfig resourceManagerConfig) {
        this.resourceManagerConfig = resourceManagerConfig;
    }

    public boolean isAmbariMonitor() {
        return ambariMonitor;
    }

    public void setAmbariMonitor(boolean ambariMonitor) {
        this.ambariMonitor = ambariMonitor;
    }

    public AmbariConfig getAmbariConfig() {
        return ambariConfig;
    }

    public void setAmbariConfig(AmbariConfig ambariConfig) {
        this.ambariConfig = ambariConfig;
    }

    public String getMetricPathPrefix() {
        return metricPathPrefix;
    }

    public void setMetricPathPrefix(String metricPathPrefix) {
        this.metricPathPrefix = metricPathPrefix;
    }

    public boolean isResourceManagerMonitor() {
        return resourceManagerMonitor;
    }

    public void setResourceManagerMonitor(boolean resourceManagerMonitor) {
        this.resourceManagerMonitor = resourceManagerMonitor;
    }
}

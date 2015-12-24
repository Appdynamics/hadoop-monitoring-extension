package com.appdynamics.monitors.hadoop.config;

import java.util.Set;

/**
 * Created by balakrishnav on 24/6/15.
 */
public class ResourceManagerConfig {
    private String hadoopVersion;
    private String host;
    private int port;
    private String username;
    private String password;
    private Set<String> mapReduceJobsToBeMonitored;
    private int monitoringTimePeriod;

    public int getMonitoringTimePeriod() {
        return monitoringTimePeriod;
    }

    public void setMonitoringTimePeriod(int monitoringTimePeriod) {
        this.monitoringTimePeriod = monitoringTimePeriod;
    }

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

    public String getHadoopVersion() {
        return hadoopVersion;
    }

    public void setHadoopVersion(String hadoopVersion) {
        this.hadoopVersion = hadoopVersion;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Set<String> getMapReduceJobsToBeMonitored() {
        return mapReduceJobsToBeMonitored;
    }

    public void setMapReduceJobsToBeMonitored(Set<String> mapReduceJobsToBeMonitored) {
        this.mapReduceJobsToBeMonitored = mapReduceJobsToBeMonitored;
    }
}

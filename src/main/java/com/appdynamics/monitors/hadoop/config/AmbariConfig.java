package com.appdynamics.monitors.hadoop.config;

import com.appdynamics.monitors.hadoop.util.HadoopMonitorUtil;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Created by balakrishnav on 24/6/15.
 */
public class AmbariConfig {
    private String host;
    private int port;
    private String username;
    private String password;
    private int numberOfThreads;

    private Set<String> includeClusters;
    private Set<String> includeHosts;
    private Set<String> includeServices;

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

    public int getNumberOfThreads() {
        if (numberOfThreads == 0) {
            numberOfThreads = HadoopMonitorUtil.DEFAULT_NUMBER_OF_THREADS;
        }
        return numberOfThreads;
    }

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public Set<String> getIncludeHosts() {
        if (includeHosts == null) {
            includeHosts = Sets.newHashSet();
        }
        return includeHosts;
    }

    public void setIncludeHosts(Set<String> includeHosts) {
        this.includeHosts = includeHosts;
    }

    public Set<String> getIncludeServices() {
        if (includeServices == null) {
            includeServices = Sets.newHashSet();
        }
        return includeServices;
    }

    public void setIncludeServices(Set<String> includeServices) {
        this.includeServices = includeServices;
    }

    public Set<String> getIncludeClusters() {
        if (includeClusters == null) {
            includeClusters = Sets.newHashSet();
        }
        return includeClusters;
    }

    public void setIncludeClusters(Set<String> includeClusters) {
        this.includeClusters = includeClusters;
    }
}

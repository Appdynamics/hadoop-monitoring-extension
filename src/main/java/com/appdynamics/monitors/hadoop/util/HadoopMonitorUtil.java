package com.appdynamics.monitors.hadoop.util;

/**
 * Created by balakrishnav on 6/7/15.
 */
public class HadoopMonitorUtil {

    public static final String AMBARI_CLUSTER_URI = "/api/v1/clusters?fields=Clusters,hosts,services";
    public static final String METRIC_SEPARATOR = "|";
    public static final int DEFAULT_NUMBER_OF_THREADS = 10;

}

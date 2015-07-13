package com.appdynamics.monitors.hadoop.hadoopexception;

/**
 * Created by balakrishnav on 6/7/15.
 */
public class ResourceManagerMonitorException extends Exception {
    public ResourceManagerMonitorException() {
    }

    public ResourceManagerMonitorException(String message) {
        super(message);
    }

    public ResourceManagerMonitorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResourceManagerMonitorException(Throwable cause) {
        super(cause);
    }
}

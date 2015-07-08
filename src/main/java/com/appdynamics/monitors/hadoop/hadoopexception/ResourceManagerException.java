package com.appdynamics.monitors.hadoop.hadoopexception;

/**
 * Created by balakrishnav on 6/7/15.
 */
public class ResourceManagerException extends Exception {
    public ResourceManagerException() {
    }

    public ResourceManagerException(String message) {
        super(message);
    }

    public ResourceManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResourceManagerException(Throwable cause) {
        super(cause);
    }
}

package com.appdynamics.monitors.hadoop.hadoopexception;

/**
 * Created by balakrishnav on 6/7/15.
 */
public class AmbariMonitorException extends Exception {
    public AmbariMonitorException() {
    }

    public AmbariMonitorException(String message) {
        super(message);
    }

    public AmbariMonitorException(String message, Throwable cause) {
        super(message, cause);
    }

    public AmbariMonitorException(Throwable cause) {
        super(cause);
    }
}

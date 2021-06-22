/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

package com.appdynamics.monitors.hadoop;

import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by balakrishnav on 24/11/15.
 */
public class HadoopMonitorTest {


    @Test
    public void hadoopMonitorTest() throws TaskExecutionException {
        HadoopMonitor monitor = new HadoopMonitor();
        final Map<String, String> taskArgs = new HashMap<>();
        taskArgs.put("config-file", "src/main/resources/conf/config.yml");
        taskArgs.put("metric-file", "src/main/resources/conf/metrics.xml");
        monitor.execute(taskArgs,null);
    }

}

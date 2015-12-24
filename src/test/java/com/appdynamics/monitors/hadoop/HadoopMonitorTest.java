package com.appdynamics.monitors.hadoop;

import com.google.common.collect.Maps;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.junit.Test;

import java.util.Map;

/**
 * Created by balakrishnav on 24/11/15.
 */
public class HadoopMonitorTest {

    public static final String CONFIG_ARG = "config-file";

    @Test
    public void testHadoopCommunicator() throws TaskExecutionException {
        Map<String, String> taskArgs = Maps.newHashMap();
        taskArgs.put(CONFIG_ARG, "src/test/resources/conf/config.yml");

        HadoopMonitor monitor = new HadoopMonitor();
        monitor.execute(taskArgs, null);
    }
}

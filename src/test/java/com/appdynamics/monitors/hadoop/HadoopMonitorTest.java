package com.appdynamics.monitors.hadoop;

import com.appdynamics.extensions.conf.MonitorConfiguration;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Created by balakrishnav on 24/11/15.
 */
public class HadoopMonitorTest {

    @Test(expected = IllegalArgumentException.class)
    public void initAmbariConfigWithNoArgs() throws Exception {
        new HadoopMonitor().initAmbariConfig(Collections.<String, String>emptyMap());
    }

    @Test
    public void initAmbariConfigCorrectArgs() throws Exception {
        Map<String, String> taskArgs = Maps.newHashMap();
        taskArgs.put(HadoopMonitor.CONFIG_ARG, "src/main/resources/conf/config.yml");
        taskArgs.put(HadoopMonitor.AMBARI_METRICS_XML_ARG, "src/main/resources/conf/metrics-ambari.xml");
        MonitorConfiguration config = new HadoopMonitor().initAmbariConfig(taskArgs);
        Assert.assertNotNull(config.getConfigYml());
        Assert.assertNotNull(config.getMetricsXmlConfiguration());
        Assert.assertEquals("Custom Metrics|HadoopMonitor|Ambari",config.getMetricPrefix());
        Assert.assertTrue(config.isEnabled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void initResourceManagerConfigWithNoArgs() throws Exception {
        new HadoopMonitor().initResourceManagerConfig(Collections.<String, String>emptyMap());
    }

    @Test
    public void initResMgrConfigCorrectArgs() throws Exception {
        Map<String, String> taskArgs = Maps.newHashMap();
        taskArgs.put(HadoopMonitor.CONFIG_ARG, "src/main/resources/conf/config.yml");
        taskArgs.put(HadoopMonitor.RM_METRICS_XML_ARG, "src/main/resources/conf/metrics-resource-manager.xml");
        MonitorConfiguration config = new HadoopMonitor().initResourceManagerConfig(taskArgs);
        Assert.assertNotNull(config.getConfigYml());
        Assert.assertNotNull(config.getMetricsXmlConfiguration());
        Assert.assertEquals("Custom Metrics|HadoopMonitor|ResourceManager",config.getMetricPrefix());
        Assert.assertTrue(config.isEnabled());
    }
}

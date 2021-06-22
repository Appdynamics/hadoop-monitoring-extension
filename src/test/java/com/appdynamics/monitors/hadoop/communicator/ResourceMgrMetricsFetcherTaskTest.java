/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

package com.appdynamics.monitors.hadoop.communicator;

import com.appdynamics.extensions.AMonitorJob;
import com.appdynamics.extensions.MetricWriteHelper;
import com.appdynamics.extensions.conf.MonitorContextConfiguration;
import com.appdynamics.monitors.hadoop.MetricOutput;
import com.appdynamics.monitors.hadoop.input.MetricConfig;
import com.appdynamics.monitors.hadoop.input.MetricStats;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.appdynamics.monitors.hadoop.communicator.AmbariMetricsFetcherTaskTest.getResourceAsJson;

/**
 * Created by abey.tom on 12/1/16.
 */
public class ResourceMgrMetricsFetcherTaskTest {
    public static final Logger logger = LoggerFactory.getLogger(ResourceMgrMetricsFetcherTaskTest.class);

    @Test
    public void resourceMgrStatsReaderTest() throws IOException {
        MetricWriteHelper writer = Mockito.mock(MetricWriteHelper.class);
        final List<MetricOutput> expected = MetricOutput.from("/data/resource-manager-expected-output.txt");
        Mockito.doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                System.out.println(arguments[0] + "," + arguments[1] + "," + arguments[2]);
                MetricOutput output = new MetricOutput((String) arguments[0], (BigDecimal) arguments[1], (String) arguments[2]);
                boolean remove = expected.remove(output);
                if (!remove) {
                    logger.error("Cannot find the value in the expected values {}", output);
                } else {
                    logger.debug("Received an expected metric " + output);
                }
                return null;
            }
        }).when(writer).printMetric(Mockito.anyString(), Mockito.any(BigDecimal.class), Mockito.anyString());
        MonitorContextConfiguration configuration = new MonitorContextConfiguration("Hadoop Monitor","Custom Metrics|Hadoop",Mockito.mock(File.class),Mockito.mock(AMonitorJob.class));
        configuration = Mockito.spy(configuration);
        configuration.setMetricXml("src/main/resources/conf/metrics.xml", MetricStats.class);
        configuration.setConfigYml("src/test/resources/conf/test-config-rm.yml");
        List<Map<String, ?>> servers = (List<Map<String, ?>>) configuration.getConfigYml().get("servers");
        Map<String,?> resourceManagerMonitor = (Map<String, ?>) configuration.getConfigYml().get("resourceManagerMonitor");
        MetricConfig metricConfig = ((MetricStats)configuration.getMetricsXml()).getMetricConfig()[0];
        ResourceMgrMetricsFetcherTask fetcher = createTask(configuration, writer, servers, resourceManagerMonitor,metricConfig);
        fetcher.run();
        Assert.assertTrue("It seems that these metrics are not reported " + expected, expected.isEmpty());
    }

    private ResourceMgrMetricsFetcherTask createTask(MonitorContextConfiguration configuration, MetricWriteHelper writer,List<Map<String, ?>> servers, Map<String,?> resourceManagerMonitor, MetricConfig metricConfig) {
        Map<String, ?> server = servers.get(0);
        ResourceMgrMetricsFetcherTask task = new ResourceMgrMetricsFetcherTask(configuration, writer, server, resourceManagerMonitor, metricConfig);
        task = Mockito.spy(task);
        Mockito.doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                String url = (String) invocation.getArguments()[0];
                if (url.endsWith("cluster/metrics")) {
                    return getResourceAsJson("/data/resourcemanager/cluster-metrics.json");
                } else if (url.endsWith("cluster/nodes")) {
                    return getResourceAsJson("/data/resourcemanager/nodes.json");
                } else if (url.endsWith("cluster/scheduler")) {
                    return getResourceAsJson("/data/resourcemanager/scheduler-fair.json");
                }else if (url.contains("cluster/apps?")) {
                    return getResourceAsJson("/data/resourcemanager/apps.json");
                }
                System.out.println(url);
                return null;
            }
        }).when(task).getResponseAsJson(Mockito.anyString());
        return task;
    }
}
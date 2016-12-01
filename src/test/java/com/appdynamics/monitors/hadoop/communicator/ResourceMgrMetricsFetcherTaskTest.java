package com.appdynamics.monitors.hadoop.communicator;

import com.appdynamics.extensions.conf.MonitorConfiguration;
import com.appdynamics.extensions.util.MetricWriteHelper;
import com.appdynamics.extensions.util.MetricWriteHelperFactory;
import com.appdynamics.monitors.hadoop.MetricOutput;
import com.appdynamics.monitors.hadoop.SynchronousExecutorService;
import com.appdynamics.monitors.hadoop.input.MetricConfig;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.appdynamics.monitors.hadoop.communicator.AmbariMetricsFetcherTaskTest.getResourceAsJson;

/**
 * Created by abey.tom on 12/1/16.
 */
public class ResourceMgrMetricsFetcherTaskTest extends AManagedMonitor {
    public static final Logger logger = LoggerFactory.getLogger(ResourceMgrMetricsFetcherTaskTest.class);

    @Test
    public void resourceMgrStatsReaderTest() throws IOException {
        Runnable runnable = Mockito.mock(Runnable.class);
        MetricWriteHelper writer = MetricWriteHelperFactory.create(this);
        writer = Mockito.spy(writer);
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
        MonitorConfiguration configuration = new MonitorConfiguration("Test", runnable, writer);
        configuration = Mockito.spy(configuration);
        Mockito.doReturn(new SynchronousExecutorService()).when(configuration).getExecutorService();
        configuration.setMetricsXml("src/main/resources/conf/metrics-resource-manager.xml", MetricConfig.class);
        configuration.setConfigYml("src/main/resources/conf/config.yml", "resourceManagerMonitor");
        List<Map<String, ?>> servers = (List<Map<String, ?>>) configuration.getConfigYml().get("servers");
        Runnable fetcher = createTask(configuration, servers);
        fetcher.run();
        Assert.assertTrue("It seems that these metrics are not reported " + expected, expected.isEmpty());
    }

    private ResourceMgrMetricsFetcherTask createTask(MonitorConfiguration configuration, List<Map<String, ?>> servers) {
        Map<String, ?> server = servers.get(0);
        ResourceMgrMetricsFetcherTask task = new ResourceMgrMetricsFetcherTask(configuration, server);
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

    public TaskOutput execute(Map<String, String> map, TaskExecutionContext taskExecutionContext) throws TaskExecutionException {
        return null;
    }
}
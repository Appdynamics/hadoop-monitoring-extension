/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Created by abey.tom on 11/10/16.
 */
public class AmbariMetricsFetcherTaskTest extends AManagedMonitor {
    public static final Logger logger = LoggerFactory.getLogger(AmbariMetricsFetcherTaskTest.class);

    @Test
    public void readClustersStat() throws JAXBException, InterruptedException, IOException {
        Runnable runnable = Mockito.mock(Runnable.class);
        MetricWriteHelper writer = MetricWriteHelperFactory.create(this);
        writer = Mockito.spy(writer);
        final List<MetricOutput> expected = MetricOutput.from("/data/ambari-expected-output.txt");
        Mockito.doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                MetricOutput output = new MetricOutput((String) arguments[0], (BigDecimal) arguments[1], (String) arguments[2]);
                boolean remove = expected.remove(output);
                if (!remove) {
                    logger.error("Cannot find the value in the expected values {}", output);
                } else{
                    logger.debug("Received an expected metric "+output);
                }
                return null;
            }
        }).when(writer).printMetric(Mockito.anyString(), Mockito.any(BigDecimal.class), Mockito.anyString());
        MonitorConfiguration configuration = new MonitorConfiguration("Test", runnable, writer);
        configuration = Mockito.spy(configuration);
        Mockito.doReturn(new SynchronousExecutorService()).when(configuration).getExecutorService();
        configuration.setMetricsXml("src/main/resources/conf/metrics-ambari.xml", MetricConfig.class);
        configuration.setConfigYml("src/main/resources/conf/config.yml", "ambariMonitor");
        List<Map<String, ?>> servers = (List<Map<String, ?>>) configuration.getConfigYml().get("servers");
        AmbariMetricsFetcherTask fetcher = createTask(configuration, servers);
        fetcher.run();
        Assert.assertTrue("It seems that these metrics are not reported " + expected, expected.isEmpty());
    }

    private AmbariMetricsFetcherTask createTask(MonitorConfiguration configuration, List<Map<String, ?>> servers) {
        AmbariMetricsFetcherTask fetcher = new AmbariMetricsFetcherTask(configuration, servers.get(0));
        fetcher = Mockito.spy(fetcher);
        Mockito.doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                String url = (String) invocation.getArguments()[0];
                if (url.contains("clusters?fields=")) {
                    return getResourceAsJson("/data/ambari/clusters.json");
                }
                //hosts and children
                else if (url.endsWith("hosts/localhost.localdomain")) {
                    return getResourceAsJson("/data/ambari/hosts-localhost.localdomain.json");
                } else if (url.endsWith("hosts/localhost.localdomain/host_components/DATANODE")) {
                    return getResourceAsJson("/data/ambari/hosts-localhost.localdomain-host_components-datanode.json");
                } else if (url.endsWith("hosts/localhost.localdomain/host_components/HBASE_MASTER")) {
                    return getResourceAsJson("/data/ambari/hosts-localhost.localdomain-host_components-hbase_master.json");
                } else if (url.endsWith("hosts/localhost.localdomain/host_components/NODEMANAGER")) {
                    return getResourceAsJson("/data/ambari/hosts-localhost.localdomain-host_components-nodemanager.json");
                }
                //Services
                else if (url.endsWith("services/AMBARI_METRICS")) {
                    return getResourceAsJson("/data/ambari/services-ambari-metrics.json");
                } else if (url.endsWith("services/HBASE")) {
                    return getResourceAsJson("/data/ambari/services-hbase.json");
                } else if (url.endsWith("services/HDFS")) {
                    return getResourceAsJson("/data/ambari/services-hdfs.json");
                } else if (url.endsWith("services/ZOOKEEPER")) {
                    return getResourceAsJson("/data/ambari/services-zookeeper.json");
                }
                // Services Components
                else if (url.endsWith("services/ZOOKEEPER/components/ZOOKEEPER_CLIENT")) {
                    return getResourceAsJson("/data/ambari/services-zookeeper-components-zookeeper_client.json");
                } else if (url.endsWith("services/ZOOKEEPER/components/ZOOKEEPER_SERVER")) {
                    return getResourceAsJson("/data/ambari/services-zookeeper-components-zookeeper_server.json");
                } else if (url.endsWith("services/HBASE/components/HBASE_CLIENT")) {
                    return getResourceAsJson("/data/ambari/services-hbase-components-hbase_client.json");
                } else if (url.endsWith("services/HBASE/components/HBASE_MASTER")) {
                    return getResourceAsJson("/data/ambari/services-hbase-components-hbase_master.json");
                } else {
                    logger.debug("There is no resource for {}", url);
                    return null;
                }
            }
        }).when(fetcher).getResponseAsJson(Mockito.anyString());
        return fetcher;
    }

    public static JsonNode getResourceAsJson(String path) throws IOException {
        InputStream in = AmbariMetricsFetcherTask.class.getResourceAsStream(path);
        return new ObjectMapper().readTree(in);
    }

    public TaskOutput execute(Map<String, String> map, TaskExecutionContext taskExecutionContext) throws TaskExecutionException {
        return null;
    }

}
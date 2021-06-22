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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by abey.tom on 11/10/16.
 */
public class AmbariMetricsFetcherTaskTest {
    public static final Logger logger = LoggerFactory.getLogger(AmbariMetricsFetcherTaskTest.class);

    @Test(timeout=45000)
    public void readClustersStat() throws JAXBException, InterruptedException, IOException {
        Runnable runnable = Mockito.mock(Runnable.class);
        MetricWriteHelper writer = Mockito.mock(MetricWriteHelper.class);
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
        MonitorContextConfiguration configuration = new MonitorContextConfiguration("Hadoop Monitor", "Custom Metrics|Hadoop Monitor",Mockito.mock(File.class),Mockito.mock(AMonitorJob.class));
        configuration = Mockito.spy(configuration);
        configuration.setMetricXml("src/main/resources/conf/metrics.xml", MetricStats.class);
        configuration.setConfigYml("src/test/resources/conf/config.yml");
        List<Map<String, ?>> servers = (List<Map<String, ?>>) configuration.getConfigYml().get("servers");
        Map<String,?> ambariMonitor = (Map<String, ?>) configuration.getConfigYml().get("ambariMonitor");
        MetricConfig metricConfig = ((MetricStats)configuration.getMetricsXml()).getMetricConfig()[1];
        AmbariMetricsFetcherTask fetcher = createTask(configuration, writer,servers,ambariMonitor,metricConfig);
        fetcher.run();
        TimeUnit.SECONDS.sleep(40);
        Assert.assertTrue("It seems that these metrics are not reported " + expected, expected.isEmpty());
    }

    private AmbariMetricsFetcherTask createTask(MonitorContextConfiguration configuration, MetricWriteHelper metricWriteHelper, List<Map<String, ?>> servers,Map<String,?> ambariMonitor, MetricConfig metricConfig) {
        AmbariMetricsFetcherTask fetcher = new AmbariMetricsFetcherTask(configuration, metricWriteHelper,servers.get(0),ambariMonitor,metricConfig);
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

}
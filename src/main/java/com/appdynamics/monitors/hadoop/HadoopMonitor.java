package com.appdynamics.monitors.hadoop;

import java.util.HashMap;
import java.util.Map;

import com.singularity.ee.agent.systemagent.api.MetricWriter;
import org.apache.log4j.Logger;

//import net.sf.json.JSON;

import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;

/**
 * Created with IntelliJ IDEA.
 * User: stephen.dong
 * Date: 9/12/13
 * Time: 1:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class HadoopMonitor extends AManagedMonitor
{

    private Map<String, String> hadoopMetrics;

    private String host;
    private String port;
    private String metricPath = "Custom Metrics|Hadoop Resource Manager|";
    HadoopCommunicator hadoopCommunicator;

    private static Logger logger = Logger.getLogger(HadoopMonitor.class);

    //for testing
    public static void main(String[] args){

        if (args.length != 2){
            System.err.println("2 arguments required: Host, Port");
            return;
        }

        HadoopMonitor hm = new HadoopMonitor();
        hm.logger = Logger.getLogger(HadoopMonitor.class);

        HadoopCommunicator hcom = new HadoopCommunicator(args[0],args[1],hm.logger);
        Map<String, String> metrics = new HashMap<String, String>();
        hcom.populate(metrics);

        for (String key: metrics.keySet()){
            System.out.println(key+" : "+metrics.get(key));
        }
    }

    public TaskOutput execute(Map<String, String> args, TaskExecutionContext arg1)
            throws TaskExecutionException
    {
        logger.info("Executing HadoopMonitor");

        if (!args.containsKey("host") || !args.containsKey("port")){
            logger.error("monitor.xml must contain task arguments 'host' and 'port'!\n" +
                        "Terminating monitor.");
            return null;
        }

        host = args.get("host");
        port = args.get("port");

        if (args.containsKey("Metric-Path") && !args.get("Metric-Path").equals("")){
            metricPath = args.get("Metric-Path");
            if (!metricPath.endsWith("|")){
                metricPath += "|";
            }
        }

        hadoopCommunicator = new HadoopCommunicator(host,port,logger);

        hadoopMetrics = new HashMap<String, String>();
        hadoopCommunicator.populate(hadoopMetrics);

        try{
            for (Map.Entry<String, String> entry : hadoopMetrics.entrySet()){
                printMetric(metricPath + entry.getKey(), entry.getValue(),
                        MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                        MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                        MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
            }
        } catch (Exception e){
            logger.error("Error printing metrics: " + e);
        }

        return new TaskOutput("Hadoop Metric Upload Complete");
//        while(true){
//            (new PrintMetricsThread()).start();
//            try{
//                Thread.sleep(60000);
//            } catch (InterruptedException e){
//                logger.error("Hadoop Resourcemanager Monitor interrupted. Quitting monitor.");
//            }
//        }

//        try
//        {
//            host = args.get("host");
//            port = args.get("port");
//
//            populate();
//
//            return new TaskOutput("Hadoop Metric Upload Complete");
//
//        }
//        catch (Exception e)
//        {
//            logger.error(e.toString());
//            return new TaskOutput("Error: " + e);
//        }
    }


    public void printMetric(String metricName, Object metricValue, String aggregation, String timeRollup, String cluster)
    {
        MetricWriter metricWriter = getMetricWriter(metricName,
                aggregation,
                timeRollup,
                cluster
        );

        metricWriter.printMetric(String.valueOf(metricValue));
    }
    //TODO: add metric filtering
}
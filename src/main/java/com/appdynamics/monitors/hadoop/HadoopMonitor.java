package com.appdynamics.monitors.hadoop;

import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import com.singularity.ee.agent.systemagent.api.MetricWriter;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

//import net.sf.json.JSON;

import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.apache.log4j.SimpleLayout;
import org.dom4j.DocumentException;

/**
 * Created with IntelliJ IDEA.
 * User: stephen.dong
 * Date: 9/12/13
 * Time: 1:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class HadoopMonitor extends AManagedMonitor
{
    private Parser xmlParser;

    private String metricPath = "Custom Metrics|";
    HadoopCommunicator hadoopCommunicator;
    AmbariCommunicator ambariCommunicator;

    private static Logger logger = Logger.getLogger(HadoopMonitor.class);

    //for testing
    public static void main(String[] args){

//        if (args.length != 2){
//            System.err.println("2 arguments required: Host, Port");
//            return;
//        }
        HadoopMonitor hm = new HadoopMonitor();
        ConsoleAppender app = new ConsoleAppender(new SimpleLayout());
        app.setName("DEFAULT");
        app.setWriter(new OutputStreamWriter(System.out));
        app.setThreshold(Level.INFO);
        org.apache.log4j.BasicConfigurator.configure(app);
        logger = Logger.getLogger(HadoopMonitor.class);

        hm.xmlParser = new Parser(logger);

        HadoopCommunicator hcom = new HadoopCommunicator(args[0],args[1],logger,hm.xmlParser);
        AmbariCommunicator acom = new AmbariCommunicator(args[2],args[3],args[4],args[5],logger,hm.xmlParser);
        Map<String, String> metrics = new HashMap<String, String>();
        hcom.populate(metrics);
        acom.populate(metrics);

        for (Map.Entry<String, String> entry: metrics.entrySet()){
            try{
                Long.parseLong(entry.getValue());
                System.out.println(entry.getKey()+" : "+entry.getValue());
            } catch (Exception e){
                logger.error("INVALID DATA: "+entry.getKey()+" : "+entry.getValue());
            }
        }
        logger.info("Metric gathering done, metric size: " + metrics.size());
    }

    public TaskOutput execute(Map<String, String> args, TaskExecutionContext arg1)
            throws TaskExecutionException
    {
        logger.info("Executing HadoopMonitor");

//        if (!args.containsKey("host") || !args.containsKey("port")){
//            logger.error("monitor.xml must contain task arguments 'host' and 'port'!\n" +
//                        "Terminating Hadoop Monitor");
//            return null;
//        }

        try {
            String host = args.get("host");
            String port = args.get("port");

            String ambariHost = args.get("ambari-user");
            String ambariPort = args.get("ambari-port");
            String ambariUser = args.get("ambari-user");
            String ambariPassword = args.get("ambari-password");


            if (args.containsKey("metric-path") && !args.get("metric-path").equals("")){
                metricPath = args.get("metric-path");
                if (!metricPath.endsWith("|")){
                    metricPath += "|";
                }
            }

            if (xmlParser == null){
                if (!args.containsKey("properties-path")){
                    logger.error("monitor.xml must contain task argument 'properties-path' describing " +
                            "the path to the XML properties file.\n" +
                            "Terminating Hadoop Monitor");
                    return null;
                }

                String xml = args.get("properties-path");
                try {
                    xmlParser = new Parser(logger, xml);
                } catch (DocumentException e) {
                    logger.error("Cannot read '" + xml + "'. Monitor is running without metric filtering\n"+
                            "Error: " + e);
                    xmlParser = new Parser(logger);
                }
    //            logger.error("user.dir is: "+System.getProperty("user.dir"));
            }

            hadoopCommunicator = new HadoopCommunicator(host,port,logger,xmlParser);
            ambariCommunicator = new AmbariCommunicator(ambariHost, ambariPort, ambariUser, ambariPassword, logger, xmlParser);

            Map<String, String> hadoopMetrics = new HashMap<String, String>();
            hadoopCommunicator.populate(hadoopMetrics);

            Map<String, String> ambariMetrics = new HashMap<String, String>();
            ambariCommunicator.populate(ambariMetrics);

            //TODO: change metric path to "Custom Metrics|<cluster name>", use ambardi metrics if there's metric overlap
            try{
                for (Map.Entry<String, String> entry : hadoopMetrics.entrySet()){
                    printMetric(metricPath + "ClusterName|" + entry.getKey(), entry.getValue(),
                            MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                            MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                            MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
                }

                for (Map.Entry<String, String> entry : ambariMetrics.entrySet()){
                    printMetric(metricPath + entry.getKey(), entry.getValue(),
                            MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                            MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                            MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
                }
            } catch (Exception e){
                logger.error("Error printing metrics: " + e);
            }

            return new TaskOutput("Hadoop Metric Upload Complete");
        } catch (Exception e) {
            logger.error(e);
            return new TaskOutput("Hadoop Metric Upload Failed");
        }
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
}
package com.appdynamics.monitors.hadoop;

import com.appdynamics.monitors.hadoop.communicator.AmbariCommunicator;
import com.appdynamics.monitors.hadoop.communicator.HadoopCommunicator;
import com.appdynamics.monitors.hadoop.parser.Parser;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.dom4j.DocumentException;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class HadoopMonitor extends AManagedMonitor
{
    private Parser xmlParser;

    private String metricPath = "Custom Metrics|Hadoop|";
    HadoopCommunicator hadoopCommunicator;
    AmbariCommunicator ambariCommunicator;

    private static Logger logger = Logger.getLogger(HadoopMonitor.class);

    //for testing
    public static void main(String[] args){
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

        try {
            String host = args.get("host");
            String port = args.get("port");

            String ambariHost = args.get("ambari-host");
            String ambariPort = args.get("ambari-port");
            String ambariUser = args.get("ambari-user");
            String ambariPassword = args.get("ambari-password");


            if (!args.get("metric-path").equals("")){
                metricPath = args.get("metric-path");
                if (!metricPath.endsWith("|")){
                    metricPath += "|";
                }
            }

            if (xmlParser == null){
                String xml = args.get("properties-path");
                try {
                    xmlParser = new Parser(logger, xml);
                } catch (DocumentException e) {
                    logger.error("Cannot read '" + xml + "'. Monitor is running without metric filtering\n"+
                            "Error: " + stackTraceToString(e));
                    xmlParser = new Parser(logger);
                }
            }

            Map<String, String> hadoopMetrics = new HashMap<String, String>();
            Map<String, String> ambariMetrics = new HashMap<String, String>();

            if (args.get("resource-manager-monitor").equals("true")){
                hadoopCommunicator = new HadoopCommunicator(host,port,logger,xmlParser);
                hadoopCommunicator.populate(hadoopMetrics);
            }
            if (args.get("ambari-monitor").equals("true")){
                ambariCommunicator = new AmbariCommunicator(ambariHost, ambariPort, ambariUser, ambariPassword, logger, xmlParser);
                ambariCommunicator.populate(ambariMetrics);
            }

            try{
                for (Map.Entry<String, String> entry : hadoopMetrics.entrySet()){
                    printMetric(metricPath + "Resource Manager|" + entry.getKey(), entry.getValue(),
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
                logger.error("Error printing metrics: " + stackTraceToString(e));
            }

            return new TaskOutput("Hadoop Metric Upload Complete");
        } catch (Exception e) {
            logger.error(stackTraceToString(e));
            return new TaskOutput("Hadoop Metric Upload Failed");
        }
    }


    private void printMetric(String metricName, Object metricValue, String aggregation, String timeRollup, String cluster)
    {
        MetricWriter metricWriter = getMetricWriter(metricName,
                aggregation,
                timeRollup,
                cluster
        );

        metricWriter.printMetric(String.valueOf(metricValue));
    }

    private String stackTraceToString(Exception e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
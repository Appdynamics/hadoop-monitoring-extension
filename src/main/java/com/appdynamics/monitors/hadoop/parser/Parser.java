package com.appdynamics.monitors.hadoop.parser;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Parser {
    private int aggrAppPeriod;
    private List<String> excludeNodeid;

    private List<String> includeAmbariCluster;
    private List<String> includeAmbariHost;
    private List<String> excludeAmbariHost;
    private List<String> excludeAmbariService;
    private List<String> excludeAmbariServiceComponent;
    private List<String> includeAmbariHostMetrics;
    private List<String> includeAmbariComponentMetrics;


    private int threadLimit = 1;

    Logger logger;

    /**
     * Constructs an empty Parser that can be populated by calling {@link #parseXML(String)}
     *
     * @param logger
     */
    public Parser(Logger logger){
        this.logger = logger;

        excludeNodeid = new ArrayList<String>();

        includeAmbariCluster = new ArrayList<String>();
        includeAmbariHost = new ArrayList<String>();
        excludeAmbariHost = new ArrayList<String>();
        excludeAmbariService = new ArrayList<String>();
        excludeAmbariServiceComponent = new ArrayList<String>();
        includeAmbariHostMetrics = new ArrayList<String>();
        includeAmbariComponentMetrics = new ArrayList<String>();
    }

    /**
     * Constructs a new Parser that's populated by filtering rules from <code>xml</code>
     *
     * @param logger
     * @param xml
     * @throws DocumentException
     */
    public Parser(Logger logger, String xml) throws DocumentException{
        this(logger);
        parseXML(xml);
    }

    /**
     * Parses XML file at <code>xml</code> and collect filtering rules.
     *
     * @param xml
     * @throws DocumentException
     */
    public void parseXML(String xml) throws DocumentException{
        SAXReader reader = new SAXReader();
        Document doc = reader.read(xml);
        Element root = doc.getRootElement();
        String text;

        Iterator<Element> hrmIter = root.element("hadoop-resource-manager").elementIterator();
        Iterator<Element> ambariIter = root.element("ambari").elementIterator();

        while(hrmIter.hasNext()){
            Element element = hrmIter.next();
            if (element.getName().equals("aggregate-app-period")){
                if (!(text = element.getText()).equals("")){
                    try {
                        aggrAppPeriod = Integer.parseInt(text);
                    } catch (NumberFormatException e){
                        logger.error("Error parsing aggregate-app-period: " + e + "\n" +
                                "Using default value instead: 15");
                        aggrAppPeriod = 15;
                    }
                }
            } else if (element.getName().equals("exclude-nodeid")){
                if (!(text = element.getText()).equals("")){

                    String[] nodeId = text.split(",");
                    excludeNodeid.addAll(Arrays.asList(nodeId));
                }
            } else {
                logger.error("Unknown element '" + element.getName() + "' in properties file");
            }
        }

        while(ambariIter.hasNext()){
            Element element = ambariIter.next();

            if (element.getName().equals("thread-limit")){
                if (!(text = element.getText()).equals("")){
                    try {
                        threadLimit = Integer.parseInt(text);
                    } catch (NumberFormatException e){
                        logger.error("Error parsing thread-limit " + e + "\n" +
                                "Using default value instead: 1");
                        threadLimit = 1;
                    }
                }
            } else if (element.getName().equals("include-cluster")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    includeAmbariCluster.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("include-host")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    includeAmbariHost.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("exclude-host")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    excludeAmbariHost.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("exclude-service")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    excludeAmbariService.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("exclude-service-component")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    excludeAmbariServiceComponent.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("include-host-metrics")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    includeAmbariHostMetrics.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("include-component-metrics")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    includeAmbariComponentMetrics.addAll(Arrays.asList(appId));
                }
            }
        }
    }

    public int getAggrAppPeriod(){
        return aggrAppPeriod;
    }

    public int getThreadLimit(){
        return threadLimit;
    }

    public boolean isIncludeNodeid(String nodeid){
        return !excludeNodeid.contains(nodeid);
    }

    public boolean isIncludeCluster(String cluster){
        return (includeAmbariCluster.contains("*") || includeAmbariCluster.contains(cluster));
    }

    public boolean isIncludeHost(String host){
        if (!excludeAmbariHost.contains("*") && !excludeAmbariHost.contains(host)){
            if (includeAmbariHost.contains("*") || includeAmbariHost.contains(host)){
                return true;
            }
        }
        return false;
    }

    public boolean isIncludeService(String service){
        return !(excludeAmbariService.contains("*") || excludeAmbariService.contains(service));
    }

    public boolean isIncludeServiceComponent(String service, String component){
        return !(excludeAmbariServiceComponent.contains("*")
                || excludeAmbariServiceComponent.contains(service + "/" + component));
    }

    public boolean isIncludeHostMetrics(String host){
        return (includeAmbariHostMetrics.contains("*") || includeAmbariHostMetrics.contains(host));
    }

    public boolean isIncludeComponentMetrics(String component){
        return (includeAmbariComponentMetrics.contains("*") || includeAmbariComponentMetrics.contains(component));
    }
}

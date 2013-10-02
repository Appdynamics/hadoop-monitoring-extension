package com.appdynamics.monitors.hadoop;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: stephen.dong
 * Date: 9/23/13
 * Time: 10:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class Parser {
    private List<String> excludeAppid;
    private List<String> excludeAppName;
    private List<String> excludeNodeid;

    private List<String> includeAmbariCluster;
    private List<String> includeAmbariHost;
    private List<String> excludeAmbariHost;
    private List<String> excludeAmbariService;
    private List<String> excludeAmbariServiceComponent;
    private List<String> includeAmbariHostMetrics;
    private List<String> includeAmbariComponentMetrics;


    private int threadLimit = 100;

    Logger logger;

    public Parser(Logger logger){
        this.logger = logger;

        excludeAppid = new ArrayList<String>();
        excludeAppName = new ArrayList<String>();
        excludeNodeid = new ArrayList<String>();

        includeAmbariCluster = new ArrayList<String>();
        includeAmbariHost = new ArrayList<String>();
        includeAmbariHost.add("*");
        excludeAmbariHost = new ArrayList<String>();
        excludeAmbariService = new ArrayList<String>();
        excludeAmbariServiceComponent = new ArrayList<String>();
        includeAmbariHostMetrics = new ArrayList<String>();
        includeAmbariComponentMetrics = new ArrayList<String>();
    }

    public Parser(Logger logger, String xml) throws DocumentException{
        this(logger);
        parseXML(xml);
    }

    public void parseXML(String xml) throws DocumentException{
        SAXReader reader = new SAXReader();
        Document doc = reader.read(xml);
        Element root = doc.getRootElement();
        String text;

        Iterator<Element> hrmIter = root.element("hadoop-resource-manager").elementIterator();
        Iterator<Element> ambariIter = root.element("ambari").elementIterator();

        while(hrmIter.hasNext()){
            Element element = hrmIter.next();

            if (element.getName().equals("exclude-appid")){
                if (!(text = element.getText()).equals("")){

                    String[] appId = text.split(",");
                    excludeAppid.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("exclude-app-name")){
                if (!(text = element.getText()).equals("")){

                    String[] appName = text.split(",");
                    excludeAppid.addAll(Arrays.asList(appName));
                }
            } else if (element.getName().equals("exclude-nodeid")){
                if (!(text = element.getText()).equals("")){

                    String[] nodeId = text.split(",");
                    excludeAppid.addAll(Arrays.asList(nodeId));
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
                        logger.error("Error parsing thread-limit " + e);
                    }
                }
            } else if (element.getName().equals("include-cluster")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    includeAmbariCluster.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("include-host")){
                if (!(text = element.getText()).equals("")){
                    if (!text.equals("*")){
                        includeAmbariHost.clear();
                        String[] appId = text.split(",");
                        includeAmbariHost.addAll(Arrays.asList(appId));
                    }
                } else {
                    includeAmbariHost.clear();
                }
            } else if (element.getName().equals("exclude-host")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    excludeAmbariHost.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("exclude-service")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.split(",");
                    excludeAmbariHost.addAll(Arrays.asList(appId));
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

    public int getThreadLimit(){
        return threadLimit;
    }

    public boolean isExcludeAppid(String appid){
        return excludeAppid.contains(appid);
    }

    public boolean isExcludeAppName(String appname){
        return excludeAppName.contains(appname);
    }

    public boolean isExcludeNodeid(String nodeid){
        return excludeNodeid.contains(nodeid);
    }

    public boolean isIncludeCluster(String cluster){
        return (includeAmbariCluster.contains("*") || includeAmbariCluster.contains(cluster));
    }

    public boolean isExcludeHost(String host){
        if (!excludeAmbariHost.contains("*") && !excludeAmbariHost.contains(host)){
            if (includeAmbariHost.contains("*") || includeAmbariHost.contains(host)){
                return false;
            }
        }
        return true;
    }

    public boolean isExcludeService(String service){
        return (excludeAmbariService.contains("*") || excludeAmbariService.contains(service));
    }

    public boolean isExcludeServiceComponent(String componentPath){
        return (excludeAmbariServiceComponent.contains("*") || excludeAmbariServiceComponent.contains(componentPath));
    }

    public boolean isIncludeHostMetrics(String host){
        return (includeAmbariHostMetrics.contains("*") || includeAmbariHostMetrics.contains(host));
    }

    public boolean isIncludeComponentMetrics(String component){
        return (includeAmbariComponentMetrics.contains("*") || includeAmbariComponentMetrics.contains(component));
    }
}

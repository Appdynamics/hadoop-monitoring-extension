/**
 * Copyright 2013 AppDynamics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.appdynamics.monitors.hadoop.parser;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Parser {
    private int aggrAppPeriod;
    private Set<String> excludeNodeid;

    private int threadLimit;
    private Set<String> includeAmbariCluster;
    private Set<String> includeAmbariHost;
    private Set<String> excludeAmbariHost;
    private Set<String> excludeAmbariService;
    private Set<String> excludeAmbariServiceComponent;
    private Set<String> includeAmbariHostMetrics;
    private Set<String> includeAmbariComponentMetrics;

    private static final int DEFAULT_THREAD_LIMIT = 1;
    private static final int DEFAULT_AGGR_APP_PERIOD = 15;

    Logger logger;

    /**
     * Constructs an empty Parser that can be populated by calling {@link #parseXML(String)}
     *
     * @param logger
     */
    public Parser(Logger logger){
        this.logger = logger;

        aggrAppPeriod = DEFAULT_AGGR_APP_PERIOD;
        excludeNodeid = new HashSet<String>();

        threadLimit = DEFAULT_THREAD_LIMIT;
        includeAmbariCluster = new HashSet<String>();
        includeAmbariHost = new HashSet<String>();
        excludeAmbariHost = new HashSet<String>();
        excludeAmbariService = new HashSet<String>();
        excludeAmbariServiceComponent = new HashSet<String>();
        includeAmbariHostMetrics = new HashSet<String>();
        includeAmbariComponentMetrics = new HashSet<String>();
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
                                "Using default value instead: " + DEFAULT_THREAD_LIMIT);
                        aggrAppPeriod = DEFAULT_AGGR_APP_PERIOD;
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
                                "Using default value instead: " + DEFAULT_THREAD_LIMIT);
                        threadLimit = DEFAULT_THREAD_LIMIT;
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
                    String[] appId = text.toLowerCase().split(",");
                    excludeAmbariService.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("exclude-service-component")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.toLowerCase().split(",");
                    excludeAmbariServiceComponent.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("include-host-metrics")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.toLowerCase().split(",");
                    includeAmbariHostMetrics.addAll(Arrays.asList(appId));
                }
            } else if (element.getName().equals("include-component-metrics")){
                if (!(text = element.getText()).equals("")){
                    String[] appId = text.toLowerCase().split(",");
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
        return !(excludeNodeid.contains("*") || excludeNodeid.contains(nodeid));
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
        return !(excludeAmbariService.contains("*")
                || excludeAmbariService.contains(service.toLowerCase()));
    }

    public boolean isIncludeServiceComponent(String service, String component){
        return !(excludeAmbariServiceComponent.contains("*")
                || excludeAmbariServiceComponent.contains(service.toLowerCase() + "/" + component.toLowerCase()));
    }

    public boolean isIncludeHostMetrics(String host){
        return (includeAmbariHostMetrics.contains("*")
                || includeAmbariHostMetrics.contains(host.toLowerCase()));
    }

    public boolean isIncludeComponentMetrics(String component){
        return (includeAmbariComponentMetrics.contains("*")
                || includeAmbariComponentMetrics.contains(component.toLowerCase()));
    }
}

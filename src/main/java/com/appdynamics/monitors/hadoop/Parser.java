package com.appdynamics.monitors.hadoop;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
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
    private List<String> excludedAppid;
    private List<String> excludedAppName;
    private List<String> excludedNodeid;

    Logger logger;

    public Parser(Logger logger){
        this.logger = logger;

        excludedAppid = new ArrayList<String>();
        excludedAppName = new ArrayList<String>();
        excludedNodeid = new ArrayList<String>();
    }

    public void parseXML(String xml) throws DocumentException{
        SAXReader reader = new SAXReader();
        Document doc = reader.read(xml);
        Element root = doc.getRootElement();
        String text;

        Iterator<Element> iter = root.elementIterator();

        while(iter.hasNext()){
            Element element = iter.next();

            if (element.getName().equals("exclude-appid")){
                if (!(text = element.getText()).equals("")){

                    String[] appid = text.split(",");
                    for (String id : appid){
                        excludedAppid.add(id);
                    }
                }
            } else if (element.getName().equals("exclude-app-name")){
                if (!(text = element.getText()).equals("")){

                    String[] appname = text.split(",");
                    for (String name : appname){
                        excludedAppName.add(name);
                    }
                }
            } else if (element.getName().equals("exclude-nodeid")){
                if (!(text = element.getText()).equals("")){

                    String[] nodeid = text.split(",");
                    for (String id : nodeid){
                        excludedNodeid.add(id);
                    }
                }
            } else {
                logger.warn("Unknown element '" + element.getName() + "' in properties.xml");
            }
        }
    }

    public List<String> getExcludedAppid(){
        return excludedAppid;
    }

    public List<String> getExcludedAppName(){
        return excludedAppName;
    }

    public List<String> getExcludedNodeid(){
        return excludedNodeid;
    }
}

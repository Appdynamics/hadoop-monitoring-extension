package com.appdynamics.monitors.hadoop.input;

import javax.xml.bind.annotation.*;

/**
 * Created by abey.tom on 3/11/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Stat {
    @XmlAttribute
    private String name;
    @XmlAttribute
    private String url;
    @XmlAttribute(name = "url-attr")
    private String urlAttr;
    @XmlAttribute
    private String label;
    @XmlAttribute(name = "metric-type")
    private String metricType;
    @XmlAttribute
    public String children;
    @XmlElement(name = "metric")
    private Metric[] metrics;
    @XmlElement(name = "naming")
    private Naming naming;
    @XmlElement(name = "stat")
    public Stat[] stats;
    @XmlElement(name = "metric-group")
    public MetricGroup[] metricGroups;
    @XmlElement(name = "filter")
    private Filter[] filters;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Metric[] getMetrics() {
        return metrics;
    }

    public void setMetrics(Metric[] metrics) {
        this.metrics = metrics;
    }

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Naming getNaming() {
        return naming;
    }

    public void setNaming(Naming naming) {
        this.naming = naming;
    }

    public Stat[] getStats() {
        return stats;
    }

    public void setStats(Stat[] stats) {
        this.stats = stats;
    }

    public String getChildren() {
        return children;
    }

    public void setChildren(String children) {
        this.children = children;
    }

    public String getUrlAttr() {
        return urlAttr;
    }

    public void setUrlAttr(String urlAttr) {
        this.urlAttr = urlAttr;
    }

    public MetricGroup[] getMetricGroups() {
        return metricGroups;
    }

    public void setMetricGroups(MetricGroup[] metricGroups) {
        this.metricGroups = metricGroups;
    }

    public Filter[] getFilters() {
        return filters;
    }

    public void setFilters(Filter[] filters) {
        this.filters = filters;
    }
}

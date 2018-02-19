/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

package com.appdynamics.monitors.hadoop.input;

import javax.xml.bind.annotation.*;

/**
 * Created by abey.tom on 9/8/16.
 */
@XmlRootElement(name = "metric-config")
@XmlAccessorType(XmlAccessType.FIELD)
public class MetricConfig {
    @XmlElement(name = "stat")
    private Stat[] stats;
    @XmlElement(name = "converter-group")
    private MetricConverterGroup[] converters;
    @XmlElement(name = "metric-group")
    private MetricGroup[] metricGroups;

    public Stat[] getStats() {
        return stats;
    }

    public void setStats(Stat[] stats) {
        this.stats = stats;
    }

    public MetricConverterGroup[] getConverters() {
        return converters;
    }

    public void setConverters(MetricConverterGroup[] converters) {
        this.converters = converters;
    }

    public MetricGroup[] getMetricGroups() {
        return metricGroups;
    }

    public void setMetricGroups(MetricGroup[] metricGroups) {
        this.metricGroups = metricGroups;
    }

    public MetricGroup getMetricGroup(String name) {
        if(metricGroups!=null){
            for (MetricGroup metricGroup : metricGroups) {
                if(name.equals(metricGroup.getName())){
                    return metricGroup;
                }
            }
        }
        return null;
    }
}

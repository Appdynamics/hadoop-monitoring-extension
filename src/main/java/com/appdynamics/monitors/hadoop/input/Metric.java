/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

package com.appdynamics.monitors.hadoop.input;

import com.appdynamics.extensions.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.*;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by abey.tom on 3/11/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Metric {
    public static final Logger logger = LoggerFactory.getLogger(Metric.class);

    @XmlAttribute
    private String attr;
    @XmlAttribute
    private String label;
    @XmlAttribute(name = "calculate-per-min")
    private Boolean calculatePerMin;
    @XmlAttribute(name = "show-only-per-min")
    private Boolean showOnlyPerMin;
    @XmlAttribute(name = "per-minute-label")
    private String perMinLabel;
    @XmlAttribute(name = "metric-type")
    private String metricType;
    @XmlAttribute(name = "per-min-metric-type")
    private String perMinMetricType;
    @XmlAttribute
    private BigDecimal multiplier;
    @XmlAttribute
    private Boolean aggregate;
    @XmlAttribute
    private String converter;
    @XmlElement(name = "converter")
    private MetricConverter[] converters;
    @XmlTransient
    private Map<String, String> converterMap;
    @XmlTransient
    private Map<String, MetricConverterGroup> converterGroupMap;


    public String getAttr() {
        return attr;
    }

    public void setAttr(String attr) {
        this.attr = attr;
    }

    public Boolean getCalculatePerMin() {
        return calculatePerMin;
    }

    public void setCalculatePerMin(Boolean calculatePerMin) {
        this.calculatePerMin = calculatePerMin;
    }

    public MetricConverter[] getConverters() {
        return converters;
    }

    public void setConverters(MetricConverter[] converters) {
        this.converters = converters;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public BigDecimal getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(BigDecimal multiplier) {
        this.multiplier = multiplier;
    }

    public String getPerMinLabel() {
        return perMinLabel;
    }

    public void setPerMinLabel(String perMinLabel) {
        this.perMinLabel = perMinLabel;
    }

    public Boolean getShowOnlyPerMin() {
        return showOnlyPerMin;
    }

    public void setShowOnlyPerMin(Boolean showOnlyPerMin) {
        this.showOnlyPerMin = showOnlyPerMin;
    }

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public String getPerMinMetricType() {
        return perMinMetricType;
    }

    public void setPerMinMetricType(String perMinMetricType) {
        this.perMinMetricType = perMinMetricType;
    }

    public Boolean getAggregate() {
        return aggregate;
    }

    public void setAggregate(Boolean aggregate) {
        this.aggregate = aggregate;
    }

    //Dirty method, assuming that these objects do not change.
    public String convertValue(String attr, String value, MetricConfig metricConfig) {
        if (converters != null && converters.length > 0) {
            if (converterMap == null) {
                converterMap = Collections.synchronizedMap(new HashMap<String, String>());
                for (MetricConverter converter : converters) {
                    converterMap.put(converter.getLabel(), converter.getValue());
                }
            }
            String converted = converterMap.get(value);
            if (StringUtils.hasText(converted)) {
                return converted;
            } else if (converterMap.containsKey("$default")) {
                return converterMap.get("$default");
            } else {
                logger.error("For the {}, the converter map {} has no value for [{}]"
                        , attr, converterMap, value);
                return value;
            }
        } else if (converter != null) {
            if (converterGroupMap == null) {
                converterGroupMap = Collections.synchronizedMap(new HashMap<String, MetricConverterGroup>());
                MetricConverterGroup[] converters = metricConfig.getConverters();
                if (converters != null) {
                    for (MetricConverterGroup converter : converters) {
                        converterGroupMap.put(converter.getName(), converter);
                    }
                }
            }
            MetricConverterGroup group = converterGroupMap.get(converter);
            if (group != null) {
                return group.convert(attr, value);
            }
        }
        return value;
    }

    public boolean hasConverters() {
        return converters != null && converters.length > 0;
    }

    public String getConverter() {
        return converter;
    }

    public void setConverter(String converter) {
        this.converter = converter;
    }
}

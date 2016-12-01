package com.appdynamics.monitors.hadoop.input;

import com.appdynamics.extensions.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by abey.tom on 9/8/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class MetricConverterGroup {
    public static final Logger logger = LoggerFactory.getLogger(MetricConverterGroup.class);

    @XmlAttribute(name = "name")
    private String name;

    @XmlElement(name = "converter")
    private MetricConverter[] converters;
    @XmlTransient
    private Map<String, String> converterMap;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MetricConverter[] getConverters() {
        return converters;
    }

    public void setConverters(MetricConverter[] converters) {
        this.converters = converters;
    }

    public String convert(String attr, String value) {
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
    }
}

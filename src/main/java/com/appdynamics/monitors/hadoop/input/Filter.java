/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

package com.appdynamics.monitors.hadoop.input;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * Created by abey.tom on 9/13/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Filter {
    @XmlAttribute
    private String name;
    @XmlAttribute(name = "url-index")
    private Integer urlIndex;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getUrlIndex() {
        return urlIndex;
    }

    public void setUrlIndex(Integer urlIndex) {
        this.urlIndex = urlIndex;
    }
}

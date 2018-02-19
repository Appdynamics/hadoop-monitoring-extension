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
 * Created by abey.tom on 3/14/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Naming {
    @XmlAttribute(name = "use-entry-name")
    private Boolean useEntryName;
    @XmlAttribute
    private String attrs;
    @XmlAttribute
    private String delimiter;

    public String getAttrs() {
        return attrs;
    }

    public void setAttrs(String attrs) {
        this.attrs = attrs;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public Boolean getUseEntryName() {
        return useEntryName;
    }

    public void setUseEntryName(Boolean useEntryName) {
        this.useEntryName = useEntryName;
    }
}

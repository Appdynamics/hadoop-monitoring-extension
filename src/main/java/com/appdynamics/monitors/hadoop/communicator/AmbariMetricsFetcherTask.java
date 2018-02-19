/*
 * Copyright 2013. AppDynamics LLC and its affiliates.
 *  * All Rights Reserved.
 *  * This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *  * The copyright notice above does not evidence any actual or intended publication of such source code.
 */

package com.appdynamics.monitors.hadoop.communicator;

import com.appdynamics.extensions.NumberUtils;
import com.appdynamics.extensions.StringUtils;
import com.appdynamics.extensions.conf.MonitorConfiguration;
import com.appdynamics.extensions.http.HttpClientUtils;
import com.appdynamics.extensions.http.UrlBuilder;
import com.appdynamics.extensions.util.JsonUtils;
import com.appdynamics.extensions.util.MetricUtils;
import com.appdynamics.extensions.util.PerMinValueCalculator;
import com.appdynamics.extensions.util.ext.AntPathMatcher;
import com.appdynamics.monitors.hadoop.input.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by abey.tom on 9/7/16.
 */
public class AmbariMetricsFetcherTask implements Runnable {
    public static final Logger logger = LoggerFactory.getLogger(AmbariMetricsFetcherTask.class);

    private MonitorConfiguration configuration;
    private Map<String, ?> server;

    public AmbariMetricsFetcherTask(MonitorConfiguration configuration, Map<String, ?> server) {
        this.configuration = configuration;
        this.server = server;
    }

    public void run() {
        try {
            String url = UrlBuilder.fromYmlServerConfig(server)
                    .path("clusters").query("fields=Clusters,hosts,services").build();
            fetchMetrics(url);
        } catch (Exception e) {
            logger.error("Unexpected error while fetching the data", e);
        }
    }

    private void fetchMetrics(String url) {
        Stat stat = getMatchingStat(url, getStats());
        if (stat != null) {
            //This filter is for the parent level stat.
            //Not the stat inside stat
            if (filterIncludes(stat, url)) {
                logger.info("Fetching the Ambari metrics from the URL {}", url);
                JsonNode response = getResponseAsJson(url);
                if (response != null) {
                    String statLabel = getStatLabel(url, stat);
                    String serverName = (String) server.get("name"); //This name could be null or Empty
                    String basePrefix = configuration.getMetricPrefix();
                    String statMetricPrefix = StringUtils.concatMetricPath(basePrefix, serverName, statLabel);
                    collectStats(stat, response, statMetricPrefix);
                } else {
                    logger.info("The url {} didnt return the expected response", url);
                }
            } else {
                logger.debug("The url {} is excluded by the filter configuration", url);
            }
        } else {
            logger.debug("Cannot find any stat that matches the url {}", url);
        }
    }

    protected JsonNode getResponseAsJson(String url) {
        return HttpClientUtils.getResponseAsJson(configuration.getHttpClient(), url, JsonNode.class);
    }

    private boolean filterIncludes(Stat stat, String url) {
        Filter[] filters = stat.getFilters();
        Map<String, ?> filtersConf = (Map<String, ?>) configuration.getConfigYml().get("filters");
        if (filters != null && filters.length > 0 & filtersConf != null) {
            String[] urlSegments = url.split("/");
            boolean include = true;
            for (Filter filter : filters) {
                String filterName = filter.getName();
                Integer urlIndex = filter.getUrlIndex();
                if (filterName != null && urlIndex != null) {
                    Map<String, ?> filterConf = (Map<String, ?>) filtersConf.get(filterName);
                    if (filterConf != null) {
                        List<String> includes = (List<String>) filterConf.get("includes");
                        if (includes != null && !includes.isEmpty()) {
                            if (!includeMatches(includes, getUrlSegmentFromIndex(urlSegments, urlIndex))) {
                                include = false;
                                logger.info("The filter include returned no match for url [{}], filterName=[{}], includes={}", url, filterName, includes);
                            }
                        }
                    }
                }
            }
            return include;
        } else {
            return true;
        }
    }

    private String getUrlSegmentFromIndex(String[] urlSegments, Integer urlIndex) {
        if (urlIndex != null) {
            int index;
            if (urlIndex < 0) {
                index = urlSegments.length + urlIndex;
            } else {
                index = urlIndex;
            }
            if (index < urlSegments.length) {
                return urlSegments[index];
            } else {
                logger.warn("The URL Index of {} is incorrect. The url is {}", Arrays.toString(urlSegments));
            }
        }
        return null;
    }

    private boolean includeMatches(List<String> includes, String name) {
        if (name != null) {
            if (includes != null && !includes.isEmpty()) {
                for (String include : includes) {
                    boolean matches = name.matches(include);
                    if (matches) {
                        return true;
                    }
                }
                //Didn't match, so return false
                return false;
            } else {
                //No filter, return true
                return true;
            }
        } else {
            //Incorrect configuration, return false.
            return false;
        }
    }

    private String getStatLabel(String url, Stat stat) {
        String label = stat.getLabel();
        String[] urlSegments = url.split("/");
        if (label != null && label.contains("{")) {
            StringBuilder sb = new StringBuilder();
            String[] labelSegments = label.split("\\|");
            for (String labelSegment : labelSegments) {
                if (labelSegment.startsWith("{") && labelSegment.endsWith("}")) {
                    String idxOrVar = labelSegment.substring(1, labelSegment.length() - 1);
                    if (NumberUtils.isNumber(idxOrVar)) {
                        int idx = Integer.parseInt(idxOrVar);
                        if (idx < 0) {
                            //Negative Index is counted from the back of the URL.
                            int index = urlSegments.length + idx;
                            if (index >= 0) {
                                sb.append(urlSegments[index]).append("|");
                            } else {
                                logger.warn("The stat label [{}] appears to be incorrect. Cannot apply [{}] on the url [{}]", label, labelSegment, url);
                            }
                        } else {
                            //Positive Index is counted from the front.
                            if (idx < urlSegments.length) {
                                sb.append(urlSegments[idx]).append("|");
                            } else {
                                logger.warn("The stat label [{}] appears to be incorrect. Cannot apply [{}] on the url [{}]", label, labelSegment, url);
                            }
                        }
                    }
                } else {
                    sb.append(labelSegment).append("|");
                }
            }
            return sb.toString();
        } else {
            return label;
        }
    }

    private Stat[] getStats() {
        MetricConfig statConf = (MetricConfig) configuration.getMetricsXmlConfiguration();
        return statConf.getStats();
    }

    private void collectStats(Stat stat, JsonNode response, String statMetricPrefix) {
        String children = stat.getChildren();
        ArrayNode nodes = getChildNodes(response, children);
        if (nodes != null) {
            for (JsonNode node : nodes) {
                collectMetrics(stat, node, statMetricPrefix);
                collectChildStats(stat, node, statMetricPrefix);
            }

        }
    }

    private void collectChildStats(Stat parentStat, JsonNode node, String statMetricPrefix) {
        if (parentStat.getStats() != null) {
            for (Stat childStat : parentStat.getStats()) {
                String urlAttr = childStat.getUrlAttr();
                if (StringUtils.hasText(urlAttr)) {
                    JsonNode nested = JsonUtils.getNestedObject(node, urlAttr.split("\\|"));
                    if (nested != null) {
                        if (nested instanceof ArrayNode) {
                            ArrayNode nestedNodes = (ArrayNode) nested;
                            for (JsonNode nestedNode : nestedNodes) {
                                String url = nestedNode.getTextValue();
                                triggerMetricsFetch(childStat, url);
                            }
                        } else {
                            String url = nested.getTextValue();
                            triggerMetricsFetch(childStat, url);
                        }
                    }
                }
                String children = childStat.getChildren();
                if (StringUtils.hasText(children)) {
                    JsonNode childNodes = JsonUtils.getNestedObject(node, children.split("\\|"));
                    String childMetricPrefix;
                    if (childStat.getLabel() != null) {
                        childMetricPrefix = StringUtils.concatMetricPath(statMetricPrefix, childStat.getLabel());
                    } else {
                        childMetricPrefix = statMetricPrefix;
                    }
                    if (childStat.getMetricType() == null) {
                        childStat.setMetricType(parentStat.getMetricType());
                    }
                    collectMetrics(childStat, childNodes, childMetricPrefix);
                }
            }
        }
    }

    private void triggerMetricsFetch(Stat childStat, final String url) {
        if (filterIncludes(childStat, url)) {
            configuration.getExecutorService().submit(new Runnable() {
                public void run() {
                    try {
                        fetchMetrics(url);
                    } catch (Exception e) {
                        logger.error("Exception while getting the data from the url " + url, e);
                    }
                }
            });
        } else {
            //The filter conf will logs the full details on the exclusion
        }
    }

    private void collectMetrics(Stat stat, JsonNode node, String statMetricPrefix) {
        Metric[] metrics = stat.getMetrics();
        if (metrics != null) {
            collectMetrics(stat, node, statMetricPrefix, metrics);
        }
        if (stat.getMetricGroups() != null) {
            MetricConfig metricConfig = (MetricConfig) configuration.getMetricsXmlConfiguration();
            for (MetricGroup group : stat.getMetricGroups()) {
                String name = group.getName();
                MetricGroup grp = metricConfig.getMetricGroup(name);
                if (grp != null) {
                    collectMetrics(stat, node, statMetricPrefix, grp.getMetrics());
                } else {
                    logger.error("Cannot find the metric group with name {}", name);
                }
            }
        }
    }

    private void collectMetrics(Stat stat, JsonNode node, String statMetricPrefix, Metric[] metrics) {
        for (Metric metric : metrics) {
            if (node instanceof ArrayNode) {
                ArrayNode nodes = (ArrayNode) node;
                for (JsonNode jsonNode : nodes) {
                    collectMetric(stat, jsonNode, statMetricPrefix, metric);
                }
            } else {
                collectMetric(stat, node, statMetricPrefix, metric);
            }
        }
    }

    private void collectMetric(Stat stat, JsonNode node, String statMetricPrefix, Metric metric) {
        String attr = metric.getAttr();
        String value = JsonUtils.getTextValue(node, attr.split("\\|"));
        if (StringUtils.hasText(value)) {
            MetricConfig metricConfig = (MetricConfig) configuration.getMetricsXmlConfiguration();
            value = metric.convertValue(attr, value, metricConfig);
            value = value.replace("%", "");
            String metricLabel = getMetricLabel(metric, node);
            String metricPath = StringUtils.concatMetricPath(statMetricPrefix, metricLabel);
            if (NumberUtils.isNumber(value)) {
                BigDecimal val = MetricUtils.multiplyAndRound(value, metric.getMultiplier());
                printMetric(metricPath, val, metric, stat);
            } else {
                logger.debug("The value for [{}] is [{}] which is not a number", metricPath, value);
            }
        } else {
            logger.warn("The attr {} didnt return any value for {}", attr, node.get("url"));
        }
    }

    private String getMetricLabel(Metric metric, JsonNode node) {
        String label = metric.getLabel();
        if (StringUtils.hasText(label)) {
            String[] segments = label.split("\\|");
            StringBuilder sb = new StringBuilder();
            for (String segment : segments) {
                if (segment.startsWith("{") && segment.endsWith("}")) {
                    String variable = segment.substring(1, segment.length() - 1);
                    String textValue = JsonUtils.getTextValue(node, variable);
                    if (textValue != null) {
                        sb.append(textValue).append("|");
                    }
                } else {
                    sb.append(segment).append("|");
                }
            }
            if (sb.length() > 0) {
                return sb.toString();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private ArrayNode getChildNodes(JsonNode response, String children) {
        JsonNode childNode;
        if (children != null) {
            childNode = JsonUtils.getNestedObject(response, children.split("\\|"));
            if (childNode == null) {
                logger.warn("The children attribute {} is not present in {}", children, response);
                return null;
            }
        } else {
            childNode = response;
        }
        ArrayNode nodes;
        if (childNode instanceof ArrayNode) {
            nodes = (ArrayNode) childNode;
        } else {
            nodes = new ObjectMapper().createArrayNode();
            nodes.add(childNode);
        }
        return nodes;
    }

    private void printMetric(String metricPath, BigDecimal value, Metric metric, Stat stat) {
        String metricType = getMetricType(metric, stat);
        if (!Boolean.TRUE.equals(metric.getShowOnlyPerMin())) {
            configuration.getMetricWriter().printMetric(metricPath, value, metricType);
        } else {
            logger.debug("Skipping the metric {}, since only perMin is needed", metricPath);
        }
        if (Boolean.TRUE.equals(metric.getCalculatePerMin())) {
            String perMinPath = metricPath + " per Min";
            PerMinValueCalculator calculator = configuration.getPerMinValueCalculator();
            BigDecimal perMinValue = calculator.getPerMinuteValue(metricPath, value);
            if (perMinValue != null) {
                String perMinMetricType = metric.getPerMinMetricType();
                if (perMinMetricType == null) {
                    perMinMetricType = metricType;
                }
                configuration.getMetricWriter().printMetric(perMinPath, perMinValue, perMinMetricType);
            }
        }
    }

    private String getMetricType(Metric metric, Stat stat) {
        if (metric.getMetricType() != null) {
            return metric.getMetricType();
        } else if (stat.getMetricType() != null) {
            return stat.getMetricType();
        } else {
            return "AVG.AVG.COL";
        }
    }


    /**
     * Order of the stat does matter, shouldn't refactor this method using HashMap, use only linked Hash Map.
     *
     * @param url
     * @param stats
     * @return
     */
    private Stat getMatchingStat(String url, Stat[] stats) {
        AntPathMatcher matcher = new AntPathMatcher();
        for (Stat stat : stats) {
            if (matcher.matches(stat.getUrl(), url)) {
                return stat;
            }
        }
        return null;
    }

}

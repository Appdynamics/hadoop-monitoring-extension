AppDynamics Apache Hadoop Monitoring Extension
==============================================

This extension works only with the standalone machine agent.

Use Case
-----------
The Hadoop monitoring extension captures metrics from Hadoop Resource Manager and/or Apache Ambari and displays them in AppDynamics Metric Browser.

Metrics include:
- Hadoop Resource Manager
  - App status and progress
  - Memory usage
  - Container usage and status
  - Node count and status
  - Scheduler capacity, app and container count
- Ambari
  - Individual host metrics including CPU, disk, memory, JVM, load, network, process, and RPC metrics
  - Service metrics for HDFS, YARN, MapReduce, HBase, Hive, WebHCat, Oozie, Ganglia, Nagios, and ZooKeeper


Installation
-------------
#### Prerequisites
- Hadoop distribution that features Hadoop YARN. Distributions based on Hadoop 2.x should include YARN
	- Examples: hadoop-2.x, hadoop-0.23.x, CDH4 from Cloudera, HDP 2 from Hortonworks, and Pivotal HD from Pivotal
- Resource Manager hostname and port  
OR
- Any release of Apache Amabri
- Any version of Hadoop cluster installed via Ambari
- Ambari server hostname, port, username, and password

If the cluster version is 1.x and not installed by Ambari, no metrics can be gathered at this time.

#### Steps
1. To build from source, clone this repository and run `mvn clean install`. This will produce a HadoopMonitor-VERSION.zip in the target directory. Alternatively, download the latest release archive from [Github](https://github.com/Appdynamics/hadoop-monitoring-extension/releases/latest).
2. Unzip as "HadoopMonitor" and copy the "HadoopMonitor" directory to `<MACHINE_AGENT_HOME>/monitors`.
3. Configure the extension by referring to the below section.
4. Open `<MACHINE_AGENT_HOME>/monitors/HadoopMonitor/config.yml` and configure the HRM and/or Ambari parameters.
5. Restart the machine agent.
6. In the AppDynamics Metric Browser, look for: Application Infrastructure Performance | \<Tier> | Custom Metrics | Hadoop.

## Configuration ##
Note : Please make sure to not use tab (\t) while editing yaml files. You may want to validate the yaml file using a [yaml validator](http://yamllint.com/)
1. Configure the extension by editing the config.yml file in `<MACHINE_AGENT_HOME>/monitors/HadoopMonitor/`.

   For eg.
   ```
    # To enable or diable ResourceManager metrics, set resourceManagerMonitor to "true" or "false"
    # The Hadoop version for the cluster you want to monitor using Resource Manager.
    # Example: 1.3, 2.2, 0.23
    # RESOURCE MANAGER Web UI CONFIG: Resource Manager is only usable for Hadoop 2.x and Hadoop 0.23.x

    resourceManagerMonitor: true
    resourceManagerConfig:
    hadoopVersion: "2.3"
    host: "localhost"
    port: 8088
    username: ""
    password: ""

    # application metrics within last X number of minutes to monitor
    monitoringTimePeriod: 15

    # mapReduceJobsToBeMonitored: comma separated jobs to be monitored within time specified by monitoringTimePeriod
    # with their state (NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED) as integers reported to controller
    mapReduceJobsToBeMonitored: ["grep-search","grep-sort"]


    # To enable or diable Ambari metrics, set ambariMonitor to "true" or "false"
    # Ambari metrics are only available for clusters inistalled using Ambari, manual installs are not eligible
    # ambariConfig: Only configure if 'ambariMonitor' is set to 'true'

    ambariMonitor: true
    ambariConfig:
    host: "192.168.0.101"
    port: 8080
    username: "admin"
    password: "admin"

    numberOfThreads: 10

    # includeClusters: comma-separated cluster names (case sensitive) you want to gather metrics for. If empty, all clusters are reported
    # includeHosts: comma-separated host names (case sensitive) you want to gather metrics for. If empty, all hosts are reported
    # includeServices: comma-separated service names (case sensitive) you want to gather metrics for. If empty, all services are reported
    includeClusters: []
    includeHosts: []
    includeServices: []

    #prefix used to show up metrics in AppDynamics
    metricPathPrefix: "Custom Metrics|HadoopMonitor|"

   ```

3. Configure the path to the config.yml file by editing the <task-arguments> in the monitor.xml file in the `<MACHINE_AGENT_HOME>/monitors/HadoopMonitor/` directory. Below is the sample

     ```
     <task-arguments>
         <!-- config file-->
         <argument name="config-file" is-required="true" default-value="monitors/HadoopMonitor/config.yml" />
          ....
     </task-arguments>
    ```


**Note** : By default, a Machine agent or a AppServer agent can send a fixed number of metrics to the controller. To change this limit, please follow the instructions mentioned [here](http://docs.appdynamics.com/display/PRO14S/Metrics+Limits).
For eg.  
```    
    java -Dappdynamics.agent.maxMetrics=2500 -jar machineagent.jar
```

Metrics
------------
### Hadoop Resource Manager: 
http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
#### ClusterMetrics
#### Nodes
#### SchedulerInfo
#### AggregatedApps
Metrics under AggregatedApps are aggregated metrics from all running and recently finished apps. To specify the period for which metrics are gathered, set `aggregateAppPeriod` in config.yml.


### Ambari
https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/index.md#monitoring
#### Host Metrics

| Metric Name   | Description
|---------------|----------------
| cpu           | CPU usage
| disk          | Disk usage (GB)
| jvm           | JVM log message count, memory usage, and thread count
| load          | Host load in 15, 5, and 1 min averages
| memory        | Memory usage (KB)
| network       | Bytes in/out, packets in/out
| process       | Process count
| rpc           | RPC process time, queue time, sent and received bytes
| rpcdetailed   | Additional RPC metrics
| ugi           | User Group Information
| part_max_used | Percent disk usage
| state         | Host state

#### Service Specific Metrics

| Metric Name   | Description
|---------------|----------------
| dfs           | HDFS specific metrics
| yarn          | YARN specific metrics
| mapred        | MapReduce specific metrics
| hbase         | HBase specific metrics

#### Host States

| State                           | Value | Description
|---------------------------------|-------|--------------
| INIT                            | 0     | New host state.
| WAITING_FOR_HOST_STATUS_UPDATES | 1     | Registration request is received from the Host but the host has not responded to its status update check.
| HEALTHY                         | 2     | The server is receiving heartbeats regularly from the host and the state of the host is healthy.
| HEARTBEAT_LOST                  | 3     | The server has not received a heartbeat from the host in the configured heartbeat expiry window.
| UNHEALTHY                       | 4     | Host is in unhealthy state as reported either by the Host itself or via any other additional means ( monitoring layer ).

#### Service, Service Component and Host Component States

| State           | Value | Description
|-----------------|-------|----------------
| INIT            | 0     | The initial clean state after the service is first created.
| INSTALLING      | 1     | In the process of installing the service.
| INSTALL_FAILED  | 2     | The service install failed.
| INSTALLED       | 3     | The service has been installed successfully but is not currently running.
| STARTING        | 4     | In the process of starting the service.
| STARTED         | 5     | The service has been installed and started.
| STOPPING        | 6     | In the process of stopping the service.
| UNINSTALLING    | 7     | In the process of uninstalling the service.
| UNINSTALLED     | 8     | The service has been successfully uninstalled.
| WIPING_OUT      | 9     | In the process of wiping out the installed service.
| UPGRADING       | 10    | In the process of upgrading the service.
| MAINTENANCE     | 11    | The service has been marked for maintenance.
| UNKNOWN         | 12    | The service state can not be determined.

#### Service component

| Metric Name | Description
|-------------|----------------
| CPU         | CPU usage
| Disk        | Disk usage (GB)
| JVM         | JVM log message count, memory usage, and thread count
| Load        | Component load in 15, 5, and 1 min averages
| Memory      | Memory usage (KB)
| Network     | Bytes in/out, packets in/out
| Process     | Process count
| RPC         | RPC process time, queue time, sent and received bytes
| RPCdetailed | Additional RPC metrics
| UGI         | User Group Information
| State       | Component state


Custom Dashboard
------------------
#### Hadoop 1

![](https://raw.github.com/Appdynamics/hadoop-monitoring-extension/master/HadoopDashboard.png)

#### Hadoop 2

![](https://raw.github.com/Appdynamics/hadoop-monitoring-extension/master/Hadoop2Dashboard.png)

Contributing
------------

Always feel free to fork and contribute any changes directly here on GitHub.


Community
---------

Find out more in the <a href="http://appsphere.appdynamics.com/t5/eXchange/Hadoop-Monitoring-Extension/idi-p/3583">AppSphere</a>.

Support
-------

For any questions or feature request, please contact <a href="mailto:help@appdynamics.com">AppDynamics Center of Excellence</a>.


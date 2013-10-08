hadoop-monitoring-extension
===========================

Apache Hadoop Monitoring Extention

Use Case
-
The Hadoop monitoring extension captures metrics from Hadoop Resource Manager and/or Apache Ambari and displays them in Appdynamics Metric Browser.

Metrics include:
- Hadoop Resource Manager
  - App status and progress; submitted, pending, running, completed, killed, and failed app count
  - Memory size, available memory
  - Allocated containers, container count in different states
  - Node status, count of nodes in different states
  - Scheduler capacity, app and container count
- Ambari
  - Individual host metrics: CPU, disk, memory, JVM, load, network, process, and RPC
  - Service component metrics: CPU, disk, memory, JVM, load, network, process, RPC, and component specific metrics



Installation
-
1. Run 'ant package' from the hadoop-monitoring-extension directory
2. Deploy the file HadoopMonitor.zip found in the 'dist' directory into \<machineagent install dir>/monitors/
3. Unzip the deployed file
4. Open \<machineagent install dir>/monitors/HadoopMonitor/monitor.xml and configure the HRM and/or Ambari parameters
5. (Optional) Configure properties.xml also under "HadoopMonitor" directory for metric filtering
6. Restart the machineagent
7. In the AppDynamics Metric Browser, look for: Application Infrastructure Performance | \<Tier> | Custom Metrics | Hadoop

Metrics
-
### HRM: 
http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html

### Ambari:
#### Host

| Metric Name | Description
|-------------|----------------
| CPU         | CPU usage
| Disk        | Disk usage (GB)
| JVM         | JVM log message count, memory usage, and thread count
| Load        | Host load in 15, 5, and 1 min averages
| Memory      | Memory usage (KB)
| Network     | Bytes in/out, packets in/out
| Process     | Process count
| RPC         | RPC process time, queue time, sent and received bytes
| RPCdetailed   | Additional RPC metrics
| UGI         | User Group Information
| part_max_used | Percent disk ussage
| State       | Host state
  
#### Host States
https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/host-resources.md#states
| State | Value | Description
|-------|-------|-----
| INIT  | 0     | New host state.
| WAITING_FOR_HOST_STATUS_UPDATES | 1 | Registration request is received from the Host but the host has not responded to its status update check.
| HEALTHY | 2 | The server is receiving heartbeats regularly from the host and the state of the host is healthy.
| HEARTBEAT_LOST | 3  |  The server has not received a heartbeat from the host in the configured heartbeat expiry window.
| UNHEALTHY | 4 | Host is in unhealthy state as reported either by the Host itself or via any other additional means ( monitoring layer ).

#### Service States
https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/service-resources.md#states

| State | Value | Description
|---|---|---
| INIT | 0 |  The initial clean state after the service is first created.
| INSTALLING | 1 |  In the process of installing the service.
| INSTALL_FAILED |2  |  The service install failed.
| INSTALLED |3 | The service has been installed successfully but is not currently running.
| STARTING | 4|  In the process of starting the service.
| STARTED | 5| The service has been installed and started.
| STOPPING | 6|  In the process of stopping the service.
| UNINSTALLING |7 |  In the process of uninstalling the service.
| UNINSTALLED | 8| The service has been successfully uninstalled.
| WIPING_OUT | 9|  In the process of wiping out the installed service.
| UPGRADING | 10| In the process of upgrading the service.
| MAINTENANCE | 11| The service has been marked for maintenance.
| UNKNOWN | 12| The service state can not be determined.

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
| RPCdetailed   | Additional RPC metrics
| UGI         | User Group Information
| State       | Component state

#### Service Component States
https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/host-component-resources.md#states

|State| Value |Description
|---|---|---
|INIT| 0| The initial clean state after the component is first created.
|INSTALLING|1 | In the process of installing the component.
|INSTALL_FAILED|2 | The component install failed.
|INSTALLED| 3|The component has been installed successfully but is not currently running.
|STARTING|4 | In the process of starting the component.
|STARTED| 5|The component has been installed and started.
|STOPPING| 6| In the process of stopping the component.
|UNINSTALLING|7 | In the process of uninstalling the component.
|UNINSTALLED| 8|The component has been successfully uninstalled.
|WIPING_OUT| 9| In the process of wiping out the installed component.
|UPGRADING| 10|In the process of upgrading the component.
|MAINTENANCE| 11|The component has been marked for maintenance.
|UNKNOWN| 12|The component state can not be determined.
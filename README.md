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
2. Deploy the file HadoopMonitor.zip found in the 'dist' directory into <machineagent install dir>/monitors/
3. Unzip the deployed file
4. Open <machineagent install dir>/monitors/HadoopMonitor/monitor.xml and configure the HRM and/or Ambari parameters
5. (Optional) Configure properties.xml also under "HadoopMonitor" directory for metric filtering
6. Restart the machineagent
7. In the AppDynamics Metric Browser, look for: Application Infrastructure Performance | \<Tier> | Custom Metrics | Hadoop

Metrics
-
HRM: http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
Ambari:
Host
  CPU       CPU usages
  Disk      
  JVM       Log message count, memory usage, thread count
  Load      
  Memory    Memory usage
  Network   Bytes in/out, packets in/out
  Process   Process count
  RPC       RPC process time, queue time, sent and received bytes
  State     Host state
  
Host States
https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/host-resources.md#states

Service States
https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/service-resources.md#states

Service component
  CPU       CPU usages
  Disk      
  JVM       Log message count, memory usage, thread count
  Load      
  Memory    Memory usage
  Network   Bytes in/out, packets in/out
  Process   Process count
  RPC       RPC process time, queue time, sent and received bytes
  State     Component state

Service Component States
https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/service-resources.md#states

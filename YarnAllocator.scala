/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:off
package org.apache.spark.deploy.yarn

import java.util.Collections
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.control.NonFatal
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef}
import org.apache.spark.scheduler.{ExecutorExited, ExecutorLossReason}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveLastAllocatedExecutorId
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}
import org.apache.spark.scheduler.TaskSetManager

import java.util
import scala.collection.JavaConversions.{asJavaCollection, asScalaBuffer, asScalaSet}
import java.net.{HttpURLConnection, URL}
import scala.util.Random
/**
 * YarnAllocator is charged with requesting containers from the YARN ResourceManager and deciding
 * what to do with containers when YARN fulfills these requests.
 *
 * This class makes use of YARN's AMRMClient APIs. We interact with the AMRMClient in three ways:
 * * Making our resource needs known, which updates local bookkeeping about containers requested.
 * * Calling "allocate", which syncs our local container requests with the RM, and returns any
 *   containers that YARN has granted to us.  This also functions as a heartbeat.
 * * Processing the containers granted to us to possibly launch executors inside of them.
 *
 * The public methods of this class are thread-safe.  All methods that mutate state are
 * synchronized.
 */
private[yarn] class YarnAllocator(
    driverUrl: String,
    driverRef: RpcEndpointRef,
    conf: YarnConfiguration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    securityMgr: SecurityManager,
    localResources: Map[String, LocalResource],
    resolver: SparkRackResolver,
    clock: Clock = new SystemClock)
  extends Logging {

  import YarnAllocator._

  // Visible for testing.
  val allocatedHostToContainersMap = new HashMap[String, collection.mutable.Set[ContainerId]]
  val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  // Containers that we no longer care about. We've either already told the RM to release them or
  // will on the next heartbeat. Containers get removed from this map after the RM tells us they've
  // completed.
  private val releasedContainers = Collections.newSetFromMap[ContainerId](
    new ConcurrentHashMap[ContainerId, java.lang.Boolean])

  private val runningExecutors = Collections.newSetFromMap[String](
    new ConcurrentHashMap[String, java.lang.Boolean]())

  private val numExecutorsStarting = new AtomicInteger(0)

  /////////////////////////////////////////////////////////////
  private val killExceedExecutors = sparkConf.getBoolean("spark.yarn.kill.exceed.executors",false)
  private val containerResourceNames = sparkConf.getOption("spark.yarn.executor.launch.hosts").map(_.split(","))
  private val containerLauncherRatio = sparkConf.getOption("spark.yarn.executor.hosts.ratio").map(_.split(":")) // 可以结合第二种方案
  private val containerUnitResource2 = sparkConf.getOption("spark.yarn.executor.hosts.resourceRatio").map(_.split(":")) // 第二种方案的探究
  private val userSpecificHostsFlag = containerResourceNames.isDefined
  private val userSpecificstyle2 = containerUnitResource2.isDefined
  // private var requestFlag = true
  private val alreadyToRunContainer: util.ArrayList[Container] = new util.ArrayList[Container]()
  private val runContainerTogetherFlag = sparkConf.getBoolean("spark.yarn.executor.launchTogether",false)
  //-----------------------------------------------------------
  // private val networkIOHost = sparkConf.get("spark.yarn.executor.io.sshHost")
  // private val networkIOTime = sparkConf.getInt("spark.yarn.executor.io.time",60) // 需要开另外的服务来获取网速流量数据了
  // private val networkValue = sparkConf.get("spark.yarn.executor.io.value","30")
  // private val runExecutorsHostsSet: mutable.Set[String] = mutable.Set()
  //-----------------------------------------------------------
  private val OptimalDynamicScheduling = sparkConf.getBoolean("spark.yarn.optimalDynamicSchedule",false)
  private var dynamicHosts = mutable.Set[String]() // 优化动态调度中辅助 pending 的获取，用一个不能包含重复元素的数据结构来
  private val alreadyRequestExecutorsName = mutable.HashMap[String,Int]() // 可变的集合
  private var containerPlacementStrategyMap = Map[String, Int]()
  /////////////////////////////////////////////////////////////

  /**
   * Used to generate a unique ID per executor
   *
   * Init `executorIdCounter`. when AM restart, `executorIdCounter` will reset to 0. Then
   * the id of new executor will start from 1, this will conflict with the executor has
   * already created before. So, we should initialize the `executorIdCounter` by getting
   * the max executorId from driver.
   *
   * And this situation of executorId conflict is just in yarn client mode, so this is an issue
   * in yarn client mode. For more details, can check in jira.
   *
   * @see SPARK-12864
   */
  private var executorIdCounter: Int =
    driverRef.askSync[Int](RetrieveLastAllocatedExecutorId)

  private[spark] val failureTracker = new FailureTracker(sparkConf, clock)

  private val allocatorBlacklistTracker =
    new YarnAllocatorBlacklistTracker(sparkConf, amClient, failureTracker)

  @volatile private var targetNumExecutors =
    SchedulerBackendUtils.getInitialTargetExecutorNumber(sparkConf)


  // Executor loss reason requests that are pending - maps from executor ID for inquiry to a
  // list of requesters that should be responded to once we find out why the given executor
  // was lost.
  private val pendingLossReasonRequests = new HashMap[String, mutable.Buffer[RpcCallContext]]

  // Maintain loss reasons for already released executors, it will be added when executor loss
  // reason is got from AM-RM call, and be removed after querying this loss reason.
  private val releasedExecutorLossReasons = new HashMap[String, ExecutorLossReason]

  // Keep track of which container is running which executor to remove the executors later
  // Visible for testing.
  private[yarn] val executorIdToContainer = new HashMap[String, Container]

  private var numUnexpectedContainerRelease = 0L
  private val containerIdToExecutorId = new HashMap[ContainerId, String]

  // Executor memory in MB.
  protected val executorMemory = sparkConf.get(EXECUTOR_MEMORY).toInt
  // Additional memory overhead.
  protected val memoryOverhead: Int = sparkConf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN)).toInt
  protected val pysparkWorkerMemory: Int = if (sparkConf.get(IS_PYTHON_APP)) {
    sparkConf.get(PYSPARK_EXECUTOR_MEMORY).map(_.toInt).getOrElse(0)
  } else {
    0
  }
  // Number of cores per executor.
  protected val executorCores = sparkConf.get(EXECUTOR_CORES)

  private val executorResourceRequests =
    sparkConf.getAllWithPrefix(config.YARN_EXECUTOR_RESOURCE_TYPES_PREFIX).toMap

  // Resource capability requested for each executor
  private[yarn] val resource: Resource = {
    val resource = Resource.newInstance(    // 这里统一化了每个executors的资源情况  可以改动申请时候的资源
      executorMemory + memoryOverhead + pysparkWorkerMemory, executorCores)
    ResourceRequestHelper.setResourceRequests(executorResourceRequests, resource)
    logInfo(s"Created resource capability: $resource")
    resource
  }

  private val launcherPool = ThreadUtils.newDaemonCachedThreadPool(
    "ContainerLauncher", sparkConf.get(CONTAINER_LAUNCH_MAX_THREADS))

  // For testing
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)

  private val labelExpression = sparkConf.get(EXECUTOR_NODE_LABEL_EXPRESSION)

  // A map to store preferred hostname and possible task numbers running on it.
  private var hostToLocalTaskCounts: Map[String, Int] = Map.empty

  // Number of tasks that have locality preferences in active stages
  private[yarn] var numLocalityAwareTasks: Int = 0

  // A container placement strategy based on pending tasks' locality preference
  private[yarn] val containerPlacementStrategy =
    new LocalityPreferredContainerPlacementStrategy(sparkConf, conf, resource, resolver)

  def getNumExecutorsRunning: Int = runningExecutors.size()

  def getNumReleasedContainers: Int = releasedContainers.size()

  def getNumExecutorsFailed: Int = failureTracker.numFailedExecutors

  def isAllNodeBlacklisted: Boolean = allocatorBlacklistTracker.isAllNodeBlacklisted

  /**
   * A sequence of pending container requests that have not yet been fulfilled.
   */
  def getPendingAllocate: Seq[ContainerRequest] = {
    // 优化动态调度需要修正的
    if(OptimalDynamicScheduling){
      val pendingContainerRequests = new ArrayBuffer[ContainerRequest]()
      if(dynamicHosts.nonEmpty){
        // logInfo(Thread.currentThread().getName + "--->(getPendingAllocate--->alreadyRequest): " + dynamicHosts)
        for (dynamicHost <- dynamicHosts) {
          pendingContainerRequests ++= getPendingAtLocation(dynamicHost) // 调用了两次？
        }
        // logInfo("neng bu neng get dao 19: " + pendingContainerRequests.size)
      }
      pendingContainerRequests
      // getPendingAtLocation(ANY_HOST) 绝对不行
    } else if(userSpecificHostsFlag){
      val pendingContainerRequests = new ArrayBuffer[ContainerRequest]()
      if(!userSpecificstyle2){
        // 异构executors情况
        containerResourceNames.get.foreach{ pendingContainerRequests ++= getPendingAtLocation(_) }
      }else {
        // 固有策略
        var i = 0
        containerResourceNames.get.foreach { location =>
          val containerUnitResource2Value = containerUnitResource2.get(i).toInt
          pendingContainerRequests ++= getPendingAtLocation(location, Resource.newInstance(executorMemory * containerUnitResource2Value, executorCores * containerUnitResource2Value))
          i = i + 1
        }
      }
//          // 可以申请 但是申请不下来=.=
//          pendingContainerRequests ++= getPendingAtLocation(location, Resource.newInstance(
//            executorMemory * 2, executorCores * 2))
       // 这里只是获取数据 并没有申请改变 所以第一次执行时是0 后面开始申请了数字才开始变化
       // logInfo("{getPendingAllocate} pendingContainerRequests.size:"+pendingContainerRequests.size)
      pendingContainerRequests
    }else{
      // 原本spark版本
      getPendingAtLocation(ANY_HOST)
    }
  }

  def numContainersPendingAllocate: Int = synchronized {
    getPendingAllocate.size
  }

  /**
   * A sequence of pending container requests at the given location that have not yet been
   * fulfilled.
   */
  private def getPendingAtLocation(location: String): Seq[ContainerRequest] = {
    amClient.getMatchingRequests(RM_REQUEST_PRIORITY, location, resource).asScala
      .flatMap(_.asScala)
      .toSeq
  }

  private def getPendingAtLocation(location: String,resource: Resource): Seq[ContainerRequest] = {
    amClient.getMatchingRequests(RM_REQUEST_PRIORITY, location, resource).asScala
      .flatMap(_.asScala)
      .toSeq
  }

  /**
   * Request as many executors from the ResourceManager as needed to reach the desired total. If
   * the requested total is smaller than the current number of running executors, no executors will
   * be killed.
   * @param requestedTotal total number of containers requested
   * @param localityAwareTasks number of locality aware tasks to be used as container placement hint
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @param nodeBlacklist blacklisted nodes, which is passed in to avoid allocating new containers
   *                      on them. It will be used to update the application master's blacklist.
   * @return Whether the new requested total is different than the old value.
   */
  def requestTotalExecutorsWithPreferredLocalities(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int],
      nodeBlacklist: Set[String]): Boolean = synchronized {
    this.numLocalityAwareTasks = localityAwareTasks
    this.hostToLocalTaskCounts = hostToLocalTaskCount

    if (requestedTotal != targetNumExecutors) {
      logInfo(s"Driver requested a total number of $requestedTotal executor(s).")
      targetNumExecutors = requestedTotal
      allocatorBlacklistTracker.setSchedulerBlacklistedNodes(nodeBlacklist)
      true
    } else {
      false
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
   */
  def killExecutor(executorId: String): Unit = synchronized {
    executorIdToContainer.get(executorId) match {
      case Some(container) if !releasedContainers.contains(container.getId) =>
        internalReleaseContainer(container)
        runningExecutors.remove(executorId)
      case _ => logWarning(s"Attempted to kill unknown executor $executorId!")
    }
  }

  /**
   * Request resources such that, if YARN gives us all we ask for, we'll have a number of containers
   * equal to maxExecutors.
   *
   * Deal with any containers YARN has granted to us by possibly launching executors in them.
   *
   * This must be synchronized because variables read in this method are mutated by other methods.
   */
  def allocateResources(): Unit = synchronized {
    updateResourceRequests()
    val progressIndicator = 0.1f
    // Poll the ResourceManager. This doubles as a heartbeat if there are no pending container requests.
    val allocateResponse = amClient.allocate(progressIndicator) // 远程调用
    // allocatedContainers是关键 分配好了位置
    val allocatedContainers = allocateResponse.getAllocatedContainers()
    allocatorBlacklistTracker.setNumClusterNodes(allocateResponse.getNumClusterNodes)
    // ......关键是个数控制 启动在哪自己已经可以控制了.....
    // v2:个数也可成功控制

    // 自己添加一些
    // TODO:1.分配好的容器先存起来 一起启动 √ 2.同步到申请过程中不要再改变pendingSize、miss的数值 √

    if(allocatedContainers.size() > 0) {
      // if (userSpecificHostsFlag) {
      logInfo("--------------RESOURCE----------------")
      //logInfo("[RS] getAllocatedContainers:" + allocatedContainers.size())
      allocatedContainers.foreach { container =>
        val nodeId = container.getNodeId
        //logInfo("[RS] " + nodeId + ": " + container.getResource)
      }
      logInfo("--------------RESOURCE----------------")

      if (!runContainerTogetherFlag)
        { handleAllocatedContainers(allocatedContainers.asScala) }
      else
      {
        logInfo("--------------runContainerTogetherFlag----------------")
        logInfo("[RS] " + runContainerTogetherFlag)
        logInfo("--------------runContainerTogetherFlag----------------")
        allocatedContainers.foreach { container =>
          alreadyToRunContainer.add(container)}
        // targetNumExecutors动态调度下是个变值var  不加targetNumExecutors!=0 0==0会反复调用handleAllocatedContainers
        if (alreadyToRunContainer.size() == targetNumExecutors && targetNumExecutors != 0) {
          logInfo("------------------------------------------------------")
          logInfo("[RS] All allocated containers start at the same time")
          logInfo("------------------------------------------------------")
          handleAllocatedContainers(alreadyToRunContainer.asScala)
          alreadyToRunContainer.clear() }
      }
    // }else{handleAllocatedContainers(allocatedContainers.asScala)}
  }

   /* if(allocatedContainers.size() > 0){
        logInfo("--------------RESOURCE----------------")
        logInfo("[RS] getAllocatedContainers:"+allocatedContainers.size())
     // var i = 0;
      allocatedContainers.foreach { container =>
        val nodeId = container.getNodeId
        logInfo("[RS] " + nodeId + ": " +container.getResource)
//        if(userSpecificstyle2){
//          logInfo("[RS] change Resource Value")
//          val containerUnitResource2Value: Int = containerUnitResource2.get(i).toInt
//          i = i + 1
//          val newResourceValue = Resource.newInstance(executorMemory * containerUnitResource2Value, executorCores * containerUnitResource2Value)
          // amClient.requestContainerResourceChange(container, newResourceValue)
//          container.setResource(newResourceValue)
//        }
      }
      logInfo("--------------RESOURCE----------------")

      // 一起启动吧 满了再启动 ? 不能一起启动的 它是一个一直循环调用更新allocatedContainers的过程 除非你能给它优化了 不进行反复调用
      // TODO：死循环的位置优化 满足动态调度回收 同时也可申请的节点同时启动（那这样好像就没必要满足动态调度回收了）

    }*/
    // 动态调度的回收代码部分
    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logInfo("Completed %d containers".format(completedContainers.size))
      processCompletedContainers(completedContainers.asScala)
      logInfo("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, runningExecutors.size))
    }
  }

  /**
   * Update the set of container requests that we will sync with the RM based on the number of
   * executors we have currently running and our target number of executors.
   * 新idea：分配完成executors后阻止反复调用  减少cpu轮询空转  这样动态调度会回收executors吗？
   * Visible for testing.
   */
  def updateResourceRequests(): Unit = {
    val pendingAllocate = getPendingAllocate
    val numPendingAllocate = pendingAllocate.size  // 第一次执行为0 第二次执行为4 getPendingAllocate执行了二次
    var missing = targetNumExecutors - numPendingAllocate -
      numExecutorsStarting.get - runningExecutors.size
    // - alreadyToRunContainer.size() // 这个集合一旦container开始全部步入运行状态 集合就要清空了！

    // 动态调度时targetNumExecutors也是一个变值  numPendingAllocate会随着启动的executors而减少 running的增加numPendingAllocate的就减少
    logInfo(s"Updating resource requests, target: $targetNumExecutors, " +
      s"pending: $numPendingAllocate, running: ${runningExecutors.size}, " +
      s"executorsStarting: ${numExecutorsStarting.get}, "+
      s"executorsAlready: ${alreadyToRunContainer.size()}")
    // containerResourceNames.get.foreach(println)

   
    if (missing > 0) {
      if(userSpecificHostsFlag){
        logInfo("[RS] numPendingAllocate:" + numPendingAllocate)
        logInfo("[RS] miss:" + missing) // 第二次后就为0 但是numPendingAllocate不一定还是4或者直接变为0 可以变为3 有一个在running

        logInfo("----------userSpecificstyle2----------")
        logInfo("[RS] " + userSpecificstyle2)
        logInfo("----------userSpecificstyle2----------")
      }else{
        logInfo("[RS] userSpecificHostsFlag: " + userSpecificHostsFlag)
      }

      if (log.isInfoEnabled()) {
        var requestContainerMessage = s"Will request $missing (missing) executor container(s), each with " +
            s"${resource.getVirtualCores} core(s) and " +
            s"${resource.getMemory} MB memory (including $memoryOverhead MB of overhead)"
        if (ResourceRequestHelper.isYarnResourceTypesAvailable() &&
            executorResourceRequests.nonEmpty) {
          requestContainerMessage ++= s" with custom resources: " + resource.toString
        }
        logInfo(requestContainerMessage)
      }

      val newLocalityRequests = new mutable.ArrayBuffer[ContainerRequest]

      if(!userSpecificHostsFlag) {
        // Split the pending container request into three groups: locality matched list, locality
        // unmatched list and non-locality list. Take the locality matched container request into
        // consideration of container placement, treat as allocated containers.
        // For locality unmatched and locality free container requests, cancel these container
        // requests, since required locality preference has been changed, recalculating using
        // container placement strategy.
        //logInfo("[RS] hostToLocalTaskCounts: "+hostToLocalTaskCounts.toString()) // 计算的数组 A map to store preferred hostname and possible task numbers running on it.
        //logInfo("[RS] alreadyRequestExecutorsName: "+alreadyRequestExecutorsName)

        // hostToLocalTaskCounts在每个阶段也是一个变值
        if(!OptimalDynamicScheduling){
          val (localRequests, staleRequests, anyHostRequests) = splitPendingAllocationsByLocality(
            hostToLocalTaskCounts, pendingAllocate)
          // cancel "stale" requests for locations that are no longer needed
          staleRequests.foreach { stale =>
            amClient.removeContainerRequest(stale)
          }
          val cancelledContainers = staleRequests.size
          if (cancelledContainers > 0) {
            logInfo(s"Canceled $cancelledContainers container request(s) (locality no longer needed)")
          }

          // consider the number of new containers and cancelled stale containers available
          val availableContainers = missing + cancelledContainers

          // to maximize locality, include requests with no locality preference that can be cancelled
          val potentialContainers = availableContainers + anyHostRequests.size


          val containerLocalityPreferences: Array[ContainerLocalityPreferences] = containerPlacementStrategy.localityOfRequestedContainers(
            potentialContainers, numLocalityAwareTasks, hostToLocalTaskCounts,
            allocatedHostToContainersMap, localRequests)
          //logInfo("[RS] containerLocalityPreferences: " + containerLocalityPreferences.size)

          // val newLocalityRequests = new mutable.ArrayBuffer[ContainerRequest]
          containerLocalityPreferences.foreach {
            case ContainerLocalityPreferences(nodes, racks) if nodes != null =>
              if (!OptimalDynamicScheduling) {
                newLocalityRequests += createContainerRequest(resource, nodes, racks)
              } else {
                //  newLocalityRequests += createContainerRequest(resource, nodes, racks)  // 动态调度的优化修改！不应该允许降低数据本地性进行申请！！！
                // 注意把relaxLocality设置为false造成的getPendingAllocation的影响
                // logInfo("[RS] >>>>>>>>>>>>OptimalDynamicScheduling>>>>>>>>>>>>")
                newLocalityRequests += createContainerRequest(resource, nodes, racks, relaxLocality = false)
                // logInfo("[RS] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
              }
            case _ =>
          }

          if (availableContainers >= newLocalityRequests.size) {
            // more containers are available than needed for locality, fill in requests for any host
            for (i <- 0 until (availableContainers - newLocalityRequests.size)) {
              if (!OptimalDynamicScheduling) {
                newLocalityRequests += createContainerRequest(resource, null, null) // 源代码只有这一句 这里基本不会执行
              } else {
                logInfo("[RS] ************OptimalDynamicScheduling************")
                newLocalityRequests += createContainerRequest(resource, null, null, relaxLocality = false)
                logInfo("[RS] ************************************************")
              }
              // newLocalityRequests += createContainerRequest(resource, null, null)  // 动态调度的优化修改！也不应该允许降低数据本地性进行申请！！！
            }
          } else {
            val numToCancel = newLocalityRequests.size - availableContainers
            // cancel some requests without locality preferences to schedule more local containers
            anyHostRequests.slice(0, numToCancel).foreach { nonLocal =>
              amClient.removeContainerRequest(nonLocal)
            }
            if (numToCancel > 0) {
              logInfo(s"Canceled $numToCancel unlocalized container requests to resubmit with locality")
            }
          }
        }else{
          // 优化动态调度
          // dynamicHosts.clear() // 辅助  这个不能直接放这 动态调度有一种bug可能 就是上一轮的申请还没批准下来 下一轮的又来了 这样这个dynamicHosts就出问题了 get不到上一轮的pending
          // 其实它并不用clear吧？？？ 无所谓的!yes yes

            // 还有待启动的executorsMap  两个Map会将数据一直同步过来
          containerPlacementStrategyMap = hostToLocalTaskCounts.map { case (key, value) =>
              val requestCount = alreadyRequestExecutorsName.getOrElse(key, 0)
              key -> (value - requestCount)
          }
          // OptimalDynamicScheduling模式下，hostToLocalTaskCounts一一对应 砍成1个数据本地性偏好的原因
          // 这种策略调度还有一些bug：也就是砍成只有1个数据本地性的方式来申请时 但是节点申请不了：1. 那个节点在其他分区上  2.节点损坏  3.资源不够
          logInfo("[RS] containerPlacementStrategyMap: "+containerPlacementStrategyMap)

          // scala 我有一个private var containerPlacementStrategyMap: Map[String, Int]
          // 然后我每次提供一个整数值i 需要实现在这个Map里随机取i个Key 且取的同时其对应的value值也要-1 如果=0时不可以选择取这个key了 我该怎么实现

          val containerPlacementStrategyList: List[String] = containerPlacementStrategyMap.toList.flatMap { case (key, value) => List.fill(value)(key) }

          val selectContainers = Random.shuffle(containerPlacementStrategyList).take(missing) // task方法太好了 missing数意外大的话也能只取能取到的
          logInfo("[RS] selectContainers: "+selectContainers)
          if(selectContainers.nonEmpty){ // 3.17 数据块多了后第一阶段得不到数据偏好的问题 不知道为什么
            for (selectContainer <- selectContainers) {
              newLocalityRequests += createContainerRequest(resource, Array(selectContainer), null, relaxLocality = false)
            }
          }else{ // 3.17 为了解决偶尔会出现的taskIdToLocations得不到数据偏好位置的问题 得不到数据偏好 这边资源申请自然也要不能根据数据偏好去定向申请
              newLocalityRequests += createContainerRequest(resource,null, null, relaxLocality = true)
          }
        }
          // 这里得按missing数来申请 missing数根据动态调度是慢慢上去的 抽象出自己的containerPlacementStrategy 下面的方法一开始的实验证明下是错的
      /*    for (hostToLocalTaskCount <- hostToLocalTaskCounts) {
            for (i <- 0 until hostToLocalTaskCount._2) {
              logInfo("[RS] hostToLocalTaskCount._1: "+hostToLocalTaskCount._1)
              newLocalityRequests += createContainerRequest(resource, Array(hostToLocalTaskCount._1), null, relaxLocality = false)
            }
          }*/
      } else {
        // 固有策略的containerPlacementStrategy
        logInfo("==================[RS]====================")
        logInfo("[RS] userSpecificHostsFlag: " +userSpecificHostsFlag)
        if(userSpecificHostsFlag) {
          // 第一次执行与第二次执行方面不同的思想（missing计算出问题的时候）
          val hosts: Array[String] = containerResourceNames.get
          //      val random = Math.abs(new java.util.Random().nextInt())
          val hostsNum = hosts.length
          //        for (i <- 0 until missing) {
          //          val index = (i + random) % hostNum  // 循环提交请求
          //          newLocalityRequests +=
          //            createContainerRequest(resource, Array(locations(index)), null,relaxLocality = false)
          //        }
          // 不降级的话申请成功了吗？ yes！！！！！！！！   成功！
          // 请求发送部分的算法
          // val containerLauncherRatioNum: Array[String] = containerLauncherRatio.get //暂时先不用
          // 这里要保证两个长度一致       合并为一个请求?
          for (i <- 0 until hostsNum) {
            val host = hosts(i)
            // val launchNum: Int = containerLauncherRatioNum(i).toInt
            val launchNum = 1
            for (j <- 0 until launchNum) {
              if (!userSpecificstyle2) {
                newLocalityRequests +=
                  createContainerRequest(resource, Array(host), null, relaxLocality = false)
              } else {
                val containerUnitResource2Value: Int = containerUnitResource2.get(i).toInt
                newLocalityRequests +=
                  createContainerRequest(Resource.newInstance(executorMemory * containerUnitResource2Value, executorCores * containerUnitResource2Value), Array(host), null, relaxLocality = false)
              }
            }
          }
          // requestFlag = false ;
          // 执行过一次后这个变量就要置为false了 避免出现miss>0意外第二次发送请求时还是一次性发这么多 而是根据具体差（失败）多少个executors再发送
          // 这里也是一个算法优化
          // 而且很久没申请下来的 需要先remove掉需求request再重新随机发送      反复调用的过程
        }
//          if(host != "gaia4"){
//            for (j <- 0 until launchNum) {
//              newLocalityRequests +=
//                createContainerRequest(resource, Array(host), null, relaxLocality = false)
//            }
//          }else {
          //  可以请求 但申请不下来
//            for (k <- 0 until launchNum) {
//              newLocalityRequests +=
//                createContainerRequest(Resource.newInstance(
//                  executorMemory * 2, executorCores * 2), Array(host), null, relaxLocality = false)
//            }
//          }
        }

        logInfo("------------REQUEST------------")
        // 固有策略时只发送一次 动态调度发送多次
        // 请求的节点就算是按照你的文件选择方式弄的 任务也分配得乱七八糟  主要是他内部算法的问题
        // 现在的优化真的不只是资源调度了还要冲任务调度了？？？ yes
        newLocalityRequests.foreach { request =>
                  //logInfo("[RS] " + request.getCapability)
                  //logInfo("[RS] " + request.getNodes)
                  // 发送的时候装进来用于判断pending就好
          val nodes: util.List[String] = request.getNodes
          if(OptimalDynamicScheduling && null!=nodes){
            val headNode = nodes.head
            dynamicHosts.add(headNode)
            //logInfo("[RS] dynamicHosts: "+dynamicHosts)
            if (!alreadyRequestExecutorsName.contains(headNode)) {
              alreadyRequestExecutorsName.put(headNode, 1)
            } else {
              alreadyRequestExecutorsName.put(headNode, alreadyRequestExecutorsName(headNode) + 1)
            }
          }

        logInfo("[RS] REQUEST:" + nodes)
        amClient.addContainerRequest(request)
//          if(request.getNodes != null ){
//            // 无论是动态调度还是固有策略 relaxLocality = false 的方式提交请求 getPendingHost都只能通过指定host来get取到  因此多增加了个dynamicHosts  数组中的一个元素便可get到
//            dynamicHosts = request.getNodes.get(0)   // getPendingHost版本 动态调度版本优化
//          }
      }
        logInfo("------------REQUEST------------")

      if (log.isInfoEnabled()) {
        val (localized, anyHost) = newLocalityRequests.partition(_.getNodes() != null)

        // 申请请求时 如果有未指定节点的请求 就走这
        if (anyHost.nonEmpty) {
          logInfo(s"Submitted ${anyHost.size} unlocalized container requests.")
        }

        // 动态调度或者修改固有策略指定节点后这个值不为null
        localized.foreach { request =>
          logInfo(s"Submitted container request for host ${hostStr(request)}.")
          // 一次申请就三个位置了   Submitted container request for host medusa002,medusa016,medusa027.
        }
      }

    } else if (numPendingAllocate > 0 && missing < 0) {
      val numToCancel = math.min(numPendingAllocate, -missing)
      logInfo(s"Canceling requests for $numToCancel executor container(s) to have a new desired " +
        s"total $targetNumExecutors executors.")
      val matchingRequests = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, resource)
      if (!matchingRequests.isEmpty) {
        matchingRequests.iterator().next().asScala
          .take(numToCancel).foreach(amClient.removeContainerRequest)
      } else {
        logWarning("Expected to find pending requests, but found none.")
      }
    }
  }

  def stop(): Unit = {
    // Forcefully shut down the launcher pool, in case this is being called in the middle of
    // container allocation. This will prevent queued executors from being started - and
    // potentially interrupt active ExecutorRunnable instaces too.
    launcherPool.shutdownNow()
  }

  private def hostStr(request: ContainerRequest): String = {
    Option(request.getNodes) match {
      case Some(nodes) => nodes.asScala.mkString(",")
      case None => "Any"
    }
  }

  /**
   * Creates a container request, handling the reflection required to use YARN features that were
   * added in recent versions.
   */
  private def createContainerRequest(
      resource: Resource,
      nodes: Array[String],
      racks: Array[String]): ContainerRequest = {
    new ContainerRequest(resource, nodes, racks, RM_REQUEST_PRIORITY, true, labelExpression.orNull)
  }

  private def createContainerRequest(
                                      resource: Resource,
                                      nodes: Array[String],
                                      racks: Array[String],
                                      relaxLocality:Boolean): ContainerRequest = {
    new ContainerRequest(resource, nodes, racks, RM_REQUEST_PRIORITY, relaxLocality, labelExpression.orNull)
  }

  /**
   * Handle containers granted by the RM by launching executors on them.
   *
   * Due to the way the YARN allocation protocol works, certain healthy race conditions can result
   * in YARN granting containers that we no longer need. In this case, we release them.
   *
   * Visible for testing.
   */
  def handleAllocatedContainers(allocatedContainers: Seq[Container]): Unit = {
    val containersToUse = new ArrayBuffer[Container](allocatedContainers.size)

    // Match incoming requests by host
    val remainingAfterHostMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- allocatedContainers) {
    matchContainerToRequest(allocatedContainer, allocatedContainer.getNodeId.getHost,
        containersToUse, remainingAfterHostMatches)
      // logInfo("[RS] containerHost:" + allocatedContainer.getNodeId.getHost + " containerId:"+allocatedContainer.getId + " containerResource:"+allocatedContainer.getResource)
      // logInfo("[RS] containerNodeHttpAddress:::"+allocatedContainer.getNodeHttpAddress)
    }

    // Match remaining by rack. Because YARN's RackResolver swallows thread interrupts
    // (see SPARK-27094), which can cause this code to miss interrupts from the AM, use
    // a separate thread to perform the operation.
    val remainingAfterRackMatches = new ArrayBuffer[Container]
    if (remainingAfterHostMatches.nonEmpty) {
      var exception: Option[Throwable] = None
      val thread = new Thread("spark-rack-resolver") {
        override def run(): Unit = {
          try {
            for (allocatedContainer <- remainingAfterHostMatches) {
              val rack = resolver.resolve(allocatedContainer.getNodeId.getHost)
              matchContainerToRequest(allocatedContainer, rack, containersToUse,
                remainingAfterRackMatches)
            }
          } catch {
            case e: Throwable =>
              exception = Some(e)
          }
        }
      }
      thread.setDaemon(true) // 启动Reporter后台线程（一开始搞错了 并不是） 进行多次循环getRm的分配 关键优化点!!! 理解清楚整个分配的过程便能越来越体现你的方案优势!
      thread.start()

      try {
        thread.join()
      } catch {
        case e: InterruptedException =>
          thread.interrupt()
          throw e
      }

      if (exception.isDefined) {
        throw exception.get
      }
    }

    // Assign remaining that are neither node-local nor rack-local
    val remainingAfterOffRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterRackMatches) {
      matchContainerToRequest(allocatedContainer, ANY_HOST, containersToUse,
        remainingAfterOffRackMatches)
    }

    if (!remainingAfterOffRackMatches.isEmpty) {
      logDebug(s"Releasing ${remainingAfterOffRackMatches.size} unneeded containers that were " +
        s"allocated to us")
      for (container <- remainingAfterOffRackMatches) {
        internalReleaseContainer(container)
      }
    }

    logInfo("Received %d containers from YARN, launching executors on %d of them."
      .format(allocatedContainers.size, containersToUse.size))

    runAllocatedContainers(containersToUse)
  }

  /**
   * Looks for requests for the given location that match the given container allocation. If it
   * finds one, removes the request so that it won't be submitted again. Places the container into
   * containersToUse or remaining.
   *
   * @param allocatedContainer container that was given to us by YARN
   * @param location resource name, either a node, rack, or *
   * @param containersToUse list of containers that will be used
   * @param remaining list of containers that will not be used
   */
  private def matchContainerToRequest(
      allocatedContainer: Container,
      location: String,
      containersToUse: ArrayBuffer[Container],
      remaining: ArrayBuffer[Container]): Unit = {
    // SPARK-6050: certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.

    // 这里也改了 改成后面使用的
//    val matchingResource = Resource.newInstance(allocatedContainer.getResource.getMemory,
//      resource.getVirtualCores)
    val matchingResource = Resource.newInstance(allocatedContainer.getResource.getMemory,
      allocatedContainer.getResource.getVirtualCores)
    ResourceRequestHelper.setResourceRequests(executorResourceRequests, matchingResource)

    logInfo(s"Calling amClient.getMatchingRequests with parameters: " +
        s"priority: ${allocatedContainer.getPriority}, " +
        s"location: $location, resource: $matchingResource")
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      matchingResource)

    // Match the allocation to a request
    if (!matchingRequests.isEmpty) {
      val containerRequest = matchingRequests.get(0).iterator.next
      logDebug(s"Removing container request via AM client: $containerRequest")
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  /**
   * Launches executors in the allocated containers.
   */
  private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]) = {

    for (container <- containersToUse) {
      executorIdCounter += 1
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      val executorId = executorIdCounter.toString
      assert(container.getResource.getMemory >= resource.getMemory)
      logInfo(s"Launching container $containerId on host $executorHostname " +
        s"for executor with ID $executorId")

      def updateInternalState(): Unit = synchronized {
        runningExecutors.add(executorId)
        numExecutorsStarting.decrementAndGet()
        executorIdToContainer(executorId) = container
        containerIdToExecutorId(container.getId) = executorId

        val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
          new HashSet[ContainerId])
        containerSet += containerId
        allocatedContainerToHostMap.put(containerId, executorHostname)
      }

      if (runningExecutors.size() < targetNumExecutors) {
        numExecutorsStarting.incrementAndGet()
        if (launchContainers) {

          // 丢到TaskSetManager.scala去了 只有非Node_Local才发送请求
//          if (networkIOHost.isDefined & !runExecutorsHostsSet.contains(executorHostname)) {
//              runExecutorsHostsSet.add(executorHostname)
//              // 发送请求  统计网络IO信息
//              val url = new URL(s"http://${networkIOHost.get}:9898/io?hostName=${executorHostname}&time=${networkIOTime}")
//              val connection = url.openConnection().asInstanceOf[HttpURLConnection]
//              connection.setRequestMethod("GET")
//              val responseCode = connection.getResponseCode
//              logInfo("[RS] getIoInfResponseCode: " + responseCode)
//              connection.disconnect()
//          }

          launcherPool.execute(new Runnable {
            override def run(): Unit = {
              try {
                new ExecutorRunnable(
                  Some(container),
                  conf,
                  sparkConf,
                  driverUrl,
                  executorId,
                  executorHostname,
                  container.getResource.getMemorySize().toInt,
                  container.getResource.getVirtualCores,
//                  executorMemory,
//                  executorCores, // 有意思 就是这里控制 也要改掉 凡是关于resource的都要变化
                  appAttemptId.getApplicationId.toString,
                  securityMgr,
                  localResources
                ).run()
                // logInfo("[RS] runExecutorHostname:" + executorHostname + " Resource:" + container.getResource)
                updateInternalState()
              } catch {
                case e: Throwable =>
                  numExecutorsStarting.decrementAndGet()
                  if (NonFatal(e)) {
                    logError(s"Failed to launch executor $executorId on container $containerId", e)
                    // Assigned container should be released immediately
                    // to avoid unnecessary resource occupation.
                    amClient.releaseAssignedContainer(containerId)
                  } else {
                    throw e
                  }
              }
            }
          })
        } else {
          // For test only
          updateInternalState()
        }
      } else {
        logInfo(("Skip launching executorRunnable as running executors count: %d " +
          "reached target executors count: %d.").format(
          runningExecutors.size, targetNumExecutors))
      }
    }
  }

  // Visible for testing.
  private[yarn] def processCompletedContainers(completedContainers: Seq[ContainerStatus]): Unit = {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId
      val alreadyReleased = releasedContainers.remove(containerId)
      val hostOpt = allocatedContainerToHostMap.get(containerId)
      val onHostStr = hostOpt.map(host => s" on host: $host").getOrElse("")
      val exitReason = if (!alreadyReleased) {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        containerIdToExecutorId.get(containerId) match {
          case Some(executorId) => runningExecutors.remove(executorId)
          case None => logWarning(s"Cannot find executorId for container: ${containerId.toString}")
        }

        logInfo("Completed container %s%s (state: %s, exit status: %s)".format(
          containerId,
          onHostStr,
          completedContainer.getState,
          completedContainer.getExitStatus))
        // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
        // there are some exit status' we shouldn't necessarily count against us, but for
        // now I think its ok as none of the containers are expected to exit.
        val exitStatus = completedContainer.getExitStatus
        val (exitCausedByApp, containerExitReason) = exitStatus match {
          case ContainerExitStatus.SUCCESS =>
            (false, s"Executor for container $containerId exited because of a YARN event (e.g., " +
              "pre-emption) and not because of an error in the running job.")
          case ContainerExitStatus.PREEMPTED =>
            // Preemption is not the fault of the running tasks, since YARN preempts containers
            // merely to do resource sharing, and tasks that fail due to preempted executors could
            // just as easily finish on any other executor. See SPARK-8167.
            (false, s"Container ${containerId}${onHostStr} was preempted.")
          // Should probably still count memory exceeded exit codes towards task failures
          case VMEM_EXCEEDED_EXIT_CODE =>
            (true, memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              VMEM_EXCEEDED_PATTERN))
          case PMEM_EXCEEDED_EXIT_CODE =>
            (true, memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              PMEM_EXCEEDED_PATTERN))
          case _ =>
            // all the failures which not covered above, like:
            // disk failure, kill by app master or resource manager, ...
            allocatorBlacklistTracker.handleResourceAllocationFailure(hostOpt)
            (true, "Container marked as failed: " + containerId + onHostStr +
              ". Exit status: " + completedContainer.getExitStatus +
              ". Diagnostics: " + completedContainer.getDiagnostics)

        }
        if (exitCausedByApp) {
          logWarning(containerExitReason)
        } else {
          logInfo(containerExitReason)
        }
        ExecutorExited(exitStatus, exitCausedByApp, containerExitReason)
      } else {
        // If we have already released this container, then it must mean
        // that the driver has explicitly requested it to be killed
        ExecutorExited(completedContainer.getExitStatus, exitCausedByApp = false,
          s"Container $containerId exited from explicit termination request.")
      }

      for {
        host <- hostOpt
        containerSet <- allocatedHostToContainersMap.get(host)
      } {
        containerSet.remove(containerId)
        if (containerSet.isEmpty) {
          allocatedHostToContainersMap.remove(host)
        } else {
          allocatedHostToContainersMap.update(host, containerSet)
        }

        allocatedContainerToHostMap.remove(containerId)
      }

      containerIdToExecutorId.remove(containerId).foreach { eid =>
        executorIdToContainer.remove(eid)
        pendingLossReasonRequests.remove(eid) match {
          case Some(pendingRequests) =>
            // Notify application of executor loss reasons so it can decide whether it should abort
            pendingRequests.foreach(_.reply(exitReason))

          case None =>
            // We cannot find executor for pending reasons. This is because completed container
            // is processed before querying pending result. We should store it for later query.
            // This is usually happened when explicitly killing a container, the result will be
            // returned in one AM-RM communication. So query RPC will be later than this completed
            // container process.
            releasedExecutorLossReasons.put(eid, exitReason)
        }
        if (!alreadyReleased) {
          // The executor could have gone away (like no route to host, node failure, etc)
          // Notify backend about the failure of the executor
          numUnexpectedContainerRelease += 1
          driverRef.send(RemoveExecutor(eid, exitReason))
        }
      }
    }
  }

  /**
   * Register that some RpcCallContext has asked the AM why the executor was lost. Note that
   * we can only find the loss reason to send back in the next call to allocateResources().
   */
  private[yarn] def enqueueGetLossReasonRequest(
      eid: String,
      context: RpcCallContext): Unit = synchronized {
    if (executorIdToContainer.contains(eid)) {
      pendingLossReasonRequests
        .getOrElseUpdate(eid, new ArrayBuffer[RpcCallContext]) += context
    } else if (releasedExecutorLossReasons.contains(eid)) {
      // Executor is already released explicitly before getting the loss reason, so directly send
      // the pre-stored lost reason
      context.reply(releasedExecutorLossReasons.remove(eid).get)
    } else {
      logWarning(s"Tried to get the loss reason for non-existent executor $eid")
      context.sendFailure(
        new SparkException(s"Fail to find loss reason for non-existent executor $eid"))
    }
  }

  private def internalReleaseContainer(container: Container): Unit = {
    releasedContainers.add(container.getId())
    amClient.releaseAssignedContainer(container.getId())
  }

  private[yarn] def getNumUnexpectedContainerRelease = numUnexpectedContainerRelease

  private[yarn] def getNumPendingLossReasonRequests: Int = synchronized {
    pendingLossReasonRequests.size
  }

  /**
   * Split the pending container requests into 3 groups based on current localities of pending
   * tasks.
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @param pendingAllocations A sequence of pending allocation container request.
   * @return A tuple of 3 sequences, first is a sequence of locality matched container
   *         requests, second is a sequence of locality unmatched container requests, and third is a
   *         sequence of locality free container requests.
   */
  private def splitPendingAllocationsByLocality(
      hostToLocalTaskCount: Map[String, Int],
      pendingAllocations: Seq[ContainerRequest]
    ): (Seq[ContainerRequest], Seq[ContainerRequest], Seq[ContainerRequest]) = {
    val localityMatched = ArrayBuffer[ContainerRequest]()
    val localityUnMatched = ArrayBuffer[ContainerRequest]()
    val localityFree = ArrayBuffer[ContainerRequest]()

    val preferredHosts = hostToLocalTaskCount.keySet
    pendingAllocations.foreach { cr =>
      val nodes = cr.getNodes
      if (nodes == null) {
        localityFree += cr
      } else if (nodes.asScala.toSet.intersect(preferredHosts).nonEmpty) {
        localityMatched += cr
      } else {
        localityUnMatched += cr
      }
    }

    (localityMatched.toSeq, localityUnMatched.toSeq, localityFree.toSeq)
  }

}

private object YarnAllocator {
  val MEM_REGEX = "[0-9.]+ [KMG]B"
  val PMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX physical memory used")
  val VMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX virtual memory used")
  val VMEM_EXCEEDED_EXIT_CODE = -103
  val PMEM_EXCEEDED_EXIT_CODE = -104

  def memLimitExceededLogMessage(diagnostics: String, pattern: Pattern): String = {
    val matcher = pattern.matcher(diagnostics)
    val diag = if (matcher.find()) " " + matcher.group() + "." else ""
    s"Container killed by YARN for exceeding memory limits. $diag " +
      "Consider boosting spark.yarn.executor.memoryOverhead or " +
      "disabling yarn.nodemanager.vmem-check-enabled because of YARN-4714."
  }
}

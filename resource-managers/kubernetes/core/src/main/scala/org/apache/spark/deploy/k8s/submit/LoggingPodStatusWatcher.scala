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
package org.apache.spark.deploy.k8s.submit

import java.util.concurrent.{CountDownLatch, TimeUnit}

<<<<<<< HEAD
import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus, Pod}
=======
import io.fabric8.kubernetes.api.model.Pod
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.deploy.k8s.KubernetesUtils._
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils

private[k8s] trait LoggingPodStatusWatcher extends Watcher[Pod] {
  def awaitCompletion(): Unit
}

/**
 * A monitor for the running Kubernetes pod of a Spark application. Status logging occurs on
 * every state change and also at an interval for liveness.
 *
 * @param appId application ID.
 * @param maybeLoggingInterval ms between each state request. If provided, must be a positive
 *                             number.
 */
private[k8s] class LoggingPodStatusWatcherImpl(
    appId: String,
    maybeLoggingInterval: Option[Long])
  extends LoggingPodStatusWatcher with Logging {

  private val podCompletedFuture = new CountDownLatch(1)
  // start timer for periodic logging
  private val scheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("logging-pod-status-watcher")
  private val logRunnable: Runnable = () => logShortStatus()

  private var pod = Option.empty[Pod]

  private def phase: String = pod.map(_.getStatus.getPhase).getOrElse("unknown")

  def start(): Unit = {
    maybeLoggingInterval.foreach { interval =>
      scheduler.scheduleAtFixedRate(logRunnable, 0, interval, TimeUnit.MILLISECONDS)
    }
  }

  override def eventReceived(action: Action, pod: Pod): Unit = {
    this.pod = Option(pod)
    action match {
      case Action.DELETED | Action.ERROR =>
        closeWatch()

      case _ =>
        logLongStatus()
        if (hasCompleted()) {
          closeWatch()
        }
    }
  }

  override def onClose(e: KubernetesClientException): Unit = {
    logDebug(s"Stopping watching application $appId with last-observed phase $phase")
    closeWatch()
  }

  private def logShortStatus() = {
    logInfo(s"Application status for $appId (phase: $phase)")
  }

  private def logLongStatus() = {
    logInfo("State changed, new state: " + pod.map(formatPodState).getOrElse("unknown"))
  }

  private def hasCompleted(): Boolean = {
    phase == "Succeeded" || phase == "Failed"
  }

  private def closeWatch(): Unit = {
    podCompletedFuture.countDown()
    scheduler.shutdown()
  }

  override def awaitCompletion(): Unit = {
    podCompletedFuture.await()
    logInfo(pod.map { p =>
      s"Container final statuses:\n\n${containersDescription(p)}"
    }.getOrElse("No containers were found in the driver pod."))
  }
<<<<<<< HEAD

  private def containersDescription(p: Pod): String = {
    p.getStatus.getContainerStatuses.asScala.map { status =>
      Seq(
        ("Container name", status.getName),
        ("Container image", status.getImage)) ++
        containerStatusDescription(status)
    }.map(formatPairsBundle).mkString("\n\n")
  }

  private def containerStatusDescription(
      containerStatus: ContainerStatus): Seq[(String, String)] = {
    val state = containerStatus.getState
    Option(state.getRunning)
      .orElse(Option(state.getTerminated))
      .orElse(Option(state.getWaiting))
      .map {
        case running: ContainerStateRunning =>
          Seq(
            ("Container state", "Running"),
            ("Container started at", formatTime(running.getStartedAt)))
        case waiting: ContainerStateWaiting =>
          Seq(
            ("Container state", "Waiting"),
            ("Pending reason", waiting.getReason))
        case terminated: ContainerStateTerminated =>
          Seq(
            ("Container state", "Terminated"),
            ("Exit code", terminated.getExitCode.toString))
        case unknown =>
          throw new SparkException(s"Unexpected container status type ${unknown.getClass}.")
        }.getOrElse(Seq(("Container state", "N/A")))
  }

  private def formatTime(time: String): String = {
    if (time != null ||  time != "") time else "N/A"
  }
=======
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
}

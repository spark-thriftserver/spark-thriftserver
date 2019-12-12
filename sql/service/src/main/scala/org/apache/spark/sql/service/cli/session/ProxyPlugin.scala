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

package org.apache.spark.sql.service.cli.session

import java.security.PrivilegedExceptionAction
import java.util.{ServiceLoader, UUID}
import java.util.concurrent._

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{ThreadUtils, Utils}

class ProxyPlugin(sparkConf: SparkConf, hadoopConf: Configuration) extends Logging {

  private val currentUGI = UserGroupInformation.getCurrentUser.getRealUser
  private val userTokenMap = new ConcurrentHashMap[String, UserGroupInformation]()
  private val sessionToUGI = new ConcurrentHashMap[UUID, UserGroupInformation]()

  private val deprecatedProviderEnabledConfigs = List(
    "spark.yarn.security.tokens.%s.enabled",
    "spark.yarn.security.credentials.%s.enabled")
  private val providerEnabledConfig = "spark.security.credentials.%s.enabled"

  private val delegationTokenProviders = loadProviders()
  logDebug("Using the following builtin delegation token providers: " +
    s"${delegationTokenProviders.keys.mkString(", ")}.")

  private val renewalExecutor: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("Credential Renewal Thread")

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
    }
  }

  /**
   * Fetch new delegation tokens for configured services.
   *
   * @return 2-tuple (credentials with new tokens, time by which the tokens must be renewed)
   */
  private def obtainDelegationTokens(): (Credentials, Long) = {
    val creds = new Credentials()
    val nextRenewal = delegationTokenProviders.values.flatMap { provider =>
      if (provider.delegationTokensRequired(sparkConf, hadoopConf)) {
        provider.obtainDelegationTokens(hadoopConf, sparkConf, creds)
      } else {
        logDebug(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
        None
      }
    }.foldLeft(Long.MaxValue)(math.min)
    (creds, nextRenewal)
  }

  // Visible for testing.
  def isProviderLoaded(serviceName: String): Boolean = {
    delegationTokenProviders.contains(serviceName)
  }

  protected def isServiceEnabled(serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)

    deprecatedProviderEnabledConfigs.foreach { pattern =>
      val deprecatedKey = pattern.format(serviceName)
      if (sparkConf.contains(deprecatedKey)) {
        logWarning(s"${deprecatedKey} is deprecated.  Please use ${key} instead.")
      }
    }

    val isEnabledDeprecated = deprecatedProviderEnabledConfigs.forall { pattern =>
      sparkConf
        .getOption(pattern.format(serviceName))
        .map(_.toBoolean)
        .getOrElse(true)
    }

    sparkConf
      .getOption(key)
      .map(_.toBoolean)
      .getOrElse(isEnabledDeprecated)
  }

  private def scheduleRenewal(ugi: UserGroupInformation, delay: Long): Unit = {
    if (sessionToUGI.containsValue(ugi)) {
      val _delay = math.max(0, delay)
      logInfo(s"Scheduling renewal in ${UIUtils.formatDuration(delay)}.")

      val renewalTask = new Runnable() {
        override def run(): Unit = {
          updateTokensTask(ugi)
        }
      }
      renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Periodic task to login to the KDC and create new delegation tokens. Re-schedules itself
   * to fetch the next set of tokens when needed.
   */
  private def updateTokensTask(ugi: UserGroupInformation): Unit = {
    try {
      val originalCreds = ugi.getCredentials
      val creds = obtainTokensAndScheduleRenewal(ugi)
      ugi.addCredentials(creds)
      val existing = ugi.getCredentials()
      existing.mergeAll(originalCreds)
      ugi.addCredentials(existing)
    } catch {
      case _: InterruptedException =>
      // Ignore, may happen if shutting down.
      case e: Exception =>
        val delay = TimeUnit.SECONDS.toMillis(sparkConf.get(CREDENTIALS_RENEWAL_RETRY_WAIT))
        logWarning(s"Failed to update tokens, will try again in ${UIUtils.formatDuration(delay)}!" +
          " If this happens too often tasks will fail.", e)
        scheduleRenewal(ugi, delay)
    }
  }

  /**
   * Obtain new delegation tokens from the available providers. Schedules a new task to fetch
   * new tokens before the new set expires.
   *
   * @return Credentials containing the new tokens.
   */
  private def obtainTokensAndScheduleRenewal(ugi: UserGroupInformation): Credentials = {
    ugi.doAs(new PrivilegedExceptionAction[Credentials]() {
      override def run(): Credentials = {
        val (creds, nextRenewal) = obtainDelegationTokens()

        // Calculate the time when new credentials should be created, based on the configured
        // ratio.
        val now = System.currentTimeMillis
        val ratio = sparkConf.get(CREDENTIALS_RENEWAL_INTERVAL_RATIO)
        val delay = (ratio * (nextRenewal - now)).toLong
        scheduleRenewal(ugi, delay)
        creds
      }
    })
  }

  private def loadProviders(): Map[String, HadoopDelegationTokenProvider] = {
    val loader = ServiceLoader.load(classOf[HadoopDelegationTokenProvider],
      Utils.getContextOrSparkClassLoader)
    val providers = mutable.ArrayBuffer[HadoopDelegationTokenProvider]()

    val iterator = loader.iterator
    while (iterator.hasNext) {
      try {
        providers += iterator.next()
      } catch {
        case t: Throwable =>
          logDebug(s"Failed to load built in provider.", t)
      }
    }

    // Filter out providers for which spark.security.credentials.{service}.enabled is false.
    providers
      .filter { p => isServiceEnabled(p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }

  def obtainTokenForProxyUGI(uuid: UUID, ugi: UserGroupInformation): Unit = {
    sessionToUGI.put(uuid, ugi)
    updateTokensTask(ugi)
  }

  def removeProxyUGI(uuid: UUID): Unit = {
    sessionToUGI.remove(uuid)
  }

}

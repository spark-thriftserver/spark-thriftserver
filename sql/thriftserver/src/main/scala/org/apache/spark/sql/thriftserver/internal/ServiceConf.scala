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

package org.apache.spark.sql.thriftserver.internal

import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for Spark Thrift Server.
////////////////////////////////////////////////////////////////////////////////////////////////////

object ServiceConf {

  val THRIFTSERVER_ASYNC = buildConf("spark.sql.thriftServer.async")
    .doc("When set to true, Spark Thrift server executes SQL queries in an asynchronous way.")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(true)

  val THRIFTSERVER_TRANSPORT_MODE = buildConf("spark.sql.thriftserver.transport.mode")
    .internal()
    .doc("Transport mode of SparkThriftServer: 1. binary  2. http")
    .version("3.1.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValue(logLevel => Set("BINARY", "HTTP").contains(logLevel),
      "Invalid value for 'spark.sql.thriftserver.transport.mode'. Valid values are " +
        "'binary', 'http'.")
    .createWithDefault("binary")

  val THRIFTSERVER_THRIFT_BIND_HOST = buildConf("spark.sql.thriftserver.thrift.bind.host")
    .internal()
    .doc("Bind host on which to run the SparkThriftServer Thrift service.")
    .version("3.1.0")
    .stringConf
    .createWithDefault("")

  val THRIFTSERVER_HTTP_PORT = buildConf("spark.sql.thriftserver.http.port")
    .internal()
    .doc("Port number of SparkThriftServer Thrift interface " +
      "when spark.sql.thriftserver.transport.mode is 'http'.")
    .version("3.1.0")
    .intConf
    .createWithDefault(10001)

  val THRIFTSERVER_HTTP_PATH = buildConf("spark.sql.thriftserver.http.path")
    .internal()
    .doc("Path component of URL endpoint when in HTTP mode.")
    .version("3.1.0")
    .stringConf
    .createWithDefault("cliservice")

  val THRIFTSERVER_MAX_MESSAGE_SIZE =
    buildConf("spark.sql.thriftserver.max.message.size")
      .internal()
      .doc("Maximum message size in bytes a SS2 server will accept.")
      .version("3.1.0")
      .intConf
      .createWithDefault(104857600)

  val THRIFTSERVER_THRIFT_HTTP_MAX_IDLE_TIME =
    buildConf("spark.sql.thriftserver.thrift.http.max.idle.time")
      .internal()
      .doc("Maximum idle time for a connection on the server when in HTTP mode.")
      .version("3.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(1800 * 1000)

  val THRIFTSERVER_THRIFT_HTTP_WORKER_KEEPALIVE_TIME =
    buildConf("spark.sql.thriftserver.thrift.http.worker.keepalive.time")
      .internal()
      .doc("Keepalive time for an idle http worker thread. " +
        "When the number of workers exceeds min workers, " +
        "excessive threads are killed after this time interval.")
      .version("3.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(60)

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_AUTH_ENABLED =
    buildConf("spark.sql.thriftserver.thrift.http.cookie.auth.enabled")
      .internal()
      .doc("When true, SparkThriftServer in HTTP transport mode," +
        " will use cookie based authentication mechanism.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_MAX_AGE =
    buildConf("spark.sql.thriftserver.thrift.http.cookie.max.age")
      .internal()
      .doc("Maximum age in seconds for server side cookie used by SS2 in HTTP mode.")
      .version("3.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(86400)

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_DOMAIN =
    buildConf("spark.sql.thriftserver.thrift.http.cookie.domain")
      .internal()
      .doc("Domain for the HS2 generated cookies")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_PATH =
    buildConf("spark.sql.thriftserver.thrift.http.cookie.path")
      .internal()
      .doc("Path for the SS2 generated cookies")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_IS_SECURE =
    buildConf("spark.sql.thriftserver.thrift.http.cookie.is.secure")
      .internal()
      .doc("Secure attribute of the SS2 generated cookie.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_IS_HTTPONLY =
    buildConf("spark.sql.thriftserver.thrift.http.cookie.is.httponly")
      .internal()
      .doc("HttpOnly attribute of the SS2 generated cookie.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_THRIFT_PORT = buildConf("spark.sql.thriftserver.thrift.port")
    .internal()
    .doc("Port number of SparkThriftServer Thrift interface " +
      "when spark.sql.thriftserver.transport.mode is 'binary'.")
    .version("3.1.0")
    .intConf
    .createWithDefault(10000)

  val THRIFTSERVER_THRIFT_SASL_QOP = buildConf("spark.sql.thriftserver.thrift.sasl.qop")
    .internal()
    .doc("Sasl QOP value; set it to one of following values to enable higher levels of" +
      "protection for SparkThriftServer communication with clients." +
      "Setting hadoop.rpc.protection to a higher level than SparkThriftServer does not" +
      "make sense in most situations. HiveServer2 ignores hadoop.rpc.protection in favor" +
      "of spark.sql.thriftserver.thrift.sasl.qop." +
      "  \"auth\" - authentication only (default)" +
      "  \"auth-int\" - authentication plus integrity protection" +
      "  \"auth-conf\" - authentication plus integrity and confidentiality protection" +
      "This is applicable only if SparkThriftServer is configured to use Kerberos authentication.")
    .version("3.1.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValue(c => Set("AUTH", "AUTH-INT", "AUTH-CONF").contains(c),
      "Invalid value for 'spark.sql.thriftserver.thrift.sasl.qop'. Valid values are " +
        "'auth', 'auth-int' and 'auth-conf'.")
    .createWithDefault("auth")

  val THRIFTSERVER_THRIFT_MIN_WORKER_THREADS =
    buildConf("spark.sql.thriftserver.thrift.min.worker.threads")
      .internal()
      .doc("Minimum number of Thrift worker threads")
      .version("3.1.0")
      .intConf
      .createWithDefault(5)

  val THRIFTSERVER_THRIFT_MAX_WORKER_THREADS =
    buildConf("spark.sql.thriftserver.thrift.max.worker.threads")
      .internal()
      .doc("Maximum number of Thrift worker threads")
      .version("3.1.0")
      .intConf
      .createWithDefault(500)


  val THRIFTSERVER_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH =
    buildConf("spark.sql.thriftserver.thrift.exponential.backoff.slot.length")
      .internal()
      .doc("Binary exponential backoff slot time for Thrift clients " +
        "during login to SparkThriftServer," +
        "for retries until hitting Thrift client timeout")
      .version("3.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(100)

  val THRIFTSERVER_THRIFT_LOGIN_TIMEOUT =
    buildConf("spark.sql.thriftserver.thrift.login.timeout")
      .internal()
      .doc("Timeout for Thrift clients during login to SparkThriftServer")
      .version("3.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(20)

  val THRIFTSERVER_THRIFT_WORKER_KEEPALIVE_TIME =
    buildConf("spark.sql.thriftserver.thrift.worker.keepalive.time")
      .internal()
      .doc("Keepalive time (in seconds) for an idle worker thread. " +
        "When the number of workers exceeds min workers, " +
        "excessive threads are killed after this time interval.")
      .version("3.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(60)

  val THRIFTSERVER_ASYNC_EXEC_THREADS =
    buildConf("spark.sql.thriftserver.async.exec.threads")
      .internal()
      .doc("Number of threads in the async thread pool for SparkThriftServer")
      .version("3.1.0")
      .intConf
      .createWithDefault(100)

  val THRIFTSERVER_ASYNC_EXEC_SHUTDOWN_TIMEOUT =
    buildConf("spark.sql.thriftserver.async.exec.shutdown.timeout")
      .internal()
      .doc("How long SparkThriftServer shutdown will wait for async threads to terminate.")
      .version("3.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(10)

  val THRIFTSERVER_ASYNC_EXEC_WAIT_QUEUE_SIZE =
    buildConf("spark.sql.thriftserver.async.exec.wait.queue.size")
      .internal()
      .doc("Size of the wait queue for async thread pool in SparkThriftServer." +
        "After hitting this limit, the async thread pool will reject new requests.")
      .version("3.1.0")
      .intConf
      .createWithDefault(100)

  val THRIFTSERVER_ASYNC_EXEC_KEEPALIVE_TIME =
    buildConf("spark.sql.thriftserver.async.exec.keepalive.time")
      .internal()
      .doc("Time that an idle SparkThriftServer async thread (from the thread pool) " +
        "will wait for a new task to arrive before terminating")
      .version("3.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(10)

  val THRIFTSERVER_LONG_POLLING_TIMEOUT =
    buildConf("spark.sql.thriftserver.long.polling.timeout")
      .internal()
      .doc("Time that SparkThriftServer will wait before responding to " +
        "asynchronous calls that use long polling")
      .version("3.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(5000)

  val THRIFTSERVER_AUTHENTICATION =
    buildConf("spark.sql.thriftserver.authentication")
      .internal()
      .doc("Client authentication types." +
        "  NONE: no authentication check" +
        "  LDAP: LDAP/AD based authentication" +
        "  KERBEROS: Kerberos/GSSAPI authentication" +
        "  CUSTOM: Custom authentication provider" +
        "          (Use with property spark.sql.thriftserver.custom.authentication.class)" +
        "  PAM: Pluggable authentication module" +
        "  NOSASL:  Raw transport")
      .version("3.1.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(c => Set("NONE", "LDAP", "KERBEROS", "CUSTOM", "PAM", "NOSASL").contains(c),
        "Invalid value for 'spark.sql.thriftserver.authentication'. Valid values are " +
          "'NONE', 'LDAP', 'KERBEROS', 'CUSTOM', 'PAM' and 'NOSASL'.")
      .createWithDefault("NONE")

  val THRIFTSERVER_ALLOW_USER_SUBSTITUTION =
    buildConf("spark.sql.thriftserver.allow.user.substitution")
      .internal()
      .doc("Allow alternate user to be specified " +
        "as part of SparkThriftServer open connection request.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_KERBEROS_KEYTAB =
    buildConf("spark.sql.thriftserver.authentication.kerberos.keytab")
      .internal()
      .doc("Kerberos keytab file for server principal")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_KERBEROS_PRINCIPAL =
    buildConf("spark.sql.thriftserver.authentication.kerberos.principal")
      .internal()
      .doc("Kerberos server principal")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SPNEGO_KEYTAB =
    buildConf("spark.sql.thriftserver.authentication.spnego.keytab")
      .internal()
      .doc("keytab file for SPNego principal, optional," +
        "typical value would look like /etc/security/keytabs/spnego.service.keytab," +
        "This keytab would be used by SparkThriftServer when Kerberos security is enabled and " +
        "HTTP transport mode is used. " +
        "This needs to be set only if SPNEGO is to be used in authentication." +
        "SPNego authentication would be honored only " +
        "if valid spark.sql.thriftserver.authentication.spnego.principal" +
        "and hspark.sql.thriftserver.authentication.spnego.keytab are specified.")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SPNEGO_PRINCIPAL =
    buildConf("spark.sql.thriftserver.authentication.spnego.principal")
      .internal()
      .doc("SPNego service principal, optional, typical value " +
        "would look like HTTP/_HOST@EXAMPLE.COM" +
        "SPNego service principal would be used by SparkThriftServer" +
        " when Kerberos security is enabled and HTTP transport mode is used." +
        "This needs to be set only if SPNEGO is to be used in authentication.")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_PLAIN_LDAP_URL =
    buildConf("spark.sql.thriftserver.authentication.ldap.url")
      .internal()
      .doc("LDAP connection URL(s)," +
        "this value could contain URLs to mutiple LDAP servers instances for HA," +
        "each LDAP URL is separated by a SPACE character. URLs are used in the " +
        " order specified until a connection is successful.")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_PLAIN_LDAP_BASEDN =
    buildConf("spark.sql.thriftserver.authentication.ldap.baseDN")
      .internal()
      .doc("LDAP base DN")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_PLAIN_LDAP_DOMAIN =
    buildConf("spark.sql.thriftserver.authentication.ldap.Domain")
      .internal()
      .doc("")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_CUSTOM_AUTHENTICATION_CLASS =
    buildConf("spark.sql.thriftserver.custom.authentication.class")
      .internal()
      .doc("Custom authentication class. Used when property" +
        "'spark.sql.thriftserver.authentication' is set to 'CUSTOM'. Provided class" +
        "must be a proper implementation of the interface" +
        "org.apache.spark.sql.thriftserver.auth.PasswdAuthenticationProvider. SparkThriftServer" +
        "will call its Authenticate(user, passed) method to authenticate requests." +
        "The implementation may optionally implement Hadoop's" +
        "org.apache.hadoop.conf.Configurable class to grab Spark's Configuration object.")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_PAM_SERVICES =
    buildConf("spark.sql.thriftserver.authentication.pam.services")
      .internal()
      .doc("List of the underlying pam services that should be used when auth type is PAM" +
        "A file with the same name must exist in /etc/pam.d")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_ENABLE_DOAS =
    buildConf("spark.sql.thriftserver.enable.doAs")
      .internal()
      .doc("Setting this property to true will have SparkThriftServer execute" +
        "Spark operations as the user making the calls to it.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_USE_SSL =
    buildConf("spark.sql.thriftserver.use.SSL")
      .internal()
      .doc("Set this to true for using SSL encryption in SparkThriftServer.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val THRIFTSERVER_SSL_KEYSTORE_PATH =
    buildConf("spark.hadoop.spark.sql.thriftserver.ssl.keystore.path")
      .internal()
      .doc("SSL certificate keystore location.")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SSL_KEYSTORE_PASSWORD =
    buildConf("spark.hadoop.spark.sql.thriftserver.ssl.keystore.password")
      .internal()
      .doc("SSL certificate keystore password.")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SSL_PROTOCOL_BLACKLIST =
    buildConf("spark.sql.thriftserver.ssl.protocol.blacklist")
      .internal()
      .doc("SSL Versions to disable for all Hive Servers")
      .version("3.1.0")
      .stringConf
      .createWithDefault("SSLv2,SSLv3")

  val THRIFTSERVER_BUILTIN_UDF_WHITELIST =
    buildConf("spark.sql.thriftserver.builtin.udf.whitelist")
      .internal()
      .doc("Comma separated list of builtin udf names allowed in queries." +
        "An empty whitelist allows all builtin udfs to be executed.  " +
        "The udf black list takes precedence over udf white list")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SESSION_CHECK_INTERVAL =
    buildConf("spark.sql.thriftserver.session.check.interval")
      .internal()
      .doc("The check interval for session/operation timeout, " +
        "which can be disabled by setting to zero or negative value.")
      .version("3.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(6 * 3600 * 1000)

  val THRIFTSERVER_IDLE_SESSION_TIMEOUT =
    buildConf("spark.sql.thriftserver.idle.session.timeout")
      .internal()
      .doc("Session will be closed when it's not accessed for this duration, " +
        "which can be disabled by setting to zero or negative value.")
      .version("3.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(7 * 24 * 3600 * 1000)

  val THRIFTSERVER_IDLE_SESSION_CHECK_OPERATION =
    buildConf("spark.sql.thriftserver.idle.session.check.operation")
      .internal()
      .doc("Session will be considered to be idle only if there is no activity," +
        " and there is no pending operation. This setting takes effect " +
        "only if session idle timeout (spark.sql.thriftserver.idle.session.timeout) and " +
        "checking (spark.sql.thriftserver.session.check.interval) are enabled.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_IDLE_OPERATION_TIMEOUT =
    buildConf("spark.sql.thriftserver.idle.operation.timeout")
      .internal()
      .doc("Operation will be closed when it's not accessed for this duration of time, " +
        "which can be disabled by setting to zero value." +
        "With positive value, it's checked for operations " +
        "     in terminal state only (FINISHED, CANCELED, CLOSED, ERROR)." +
        "With negative value, it's checked for all of the operations regardless of state.")
      .version("3.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(5 * 24 * 60 * 60 * 1000)

  val THRIFTSERVER_LOGGING_OPERATION_ENABLE =
    buildConf("spark.sql.thriftserver.logging.operation.enabled")
      .internal()
      .doc("When true, HS2 will save operation logs and make them available for clients")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_LOGGING_OPERATION_LOG_LOCATION =
    buildConf("spark.sql.thriftserver.logging.operation.log.location")
      .internal()
      .doc("Top level directory where operation logs are stored " +
        "if logging functionality is enabled")
      .version("3.1.0")
      .stringConf
      .createWithDefault("${system:java.io.tmpdir}" + File.separator +
        "${system:user.name}" + File.separator + "operation_logs")

  val THRIFTSERVER_LOGGING_OPERATION_LEVEL =
    buildConf("spark.sql.thriftserver.logging.operation.level")
      .internal()
      .doc("HS2 operation logging mode available to clients to be set at session level." +
        "For this to work, spark.sql.thriftserver.logging.operation.enabled " +
        "should be set to true." +
        "   NONE: Ignore any logging" +
        "   EXECUTION: Log completion of tasks" +
        "   PERFORMANCE: Execution + Performance logs " +
        "   VERBOSE: All logs")
      .version("3.1.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(c => Set("NONE", "EXECUTION", "PERFORMANCE", "VERBOSE").contains(c),
        "Invalid value for 'spark.sql.thriftserver.logging.operation.level'. Valid values are " +
          "'NONE', 'EXECUTION', 'PERFORMANCE' and 'VERBOSE'.")
      .createWithDefault("EXECUTION")

  val THRIFTSERVER_GLOABLE_INIT_FILE_LOCATION =
    buildConf("spark.sql.thriftserver.global.init.file.location")
      .internal()
      .doc("Either the location of a SparkThriftServer global init file or a directory" +
        " containing a .sparkrc file. If the property is set, the value must be a valid path " +
        "to an init file or directory where the init file is located.")
      .version("3.1.0")
      .stringConf
      .createWithDefault("${env:SPARK_CONF_DIR}")

  def isAsync(conf: SQLConf): Boolean = conf.getConf(THRIFTSERVER_ASYNC)

  def transportMode(conf: SQLConf): String = conf.getConf(THRIFTSERVER_TRANSPORT_MODE)

  def thriftBindHost(conf: SQLConf): String = conf.getConf(THRIFTSERVER_THRIFT_BIND_HOST)

  def httpPort(conf: SQLConf): Int = conf.getConf(THRIFTSERVER_HTTP_PORT)

  def httpPath(conf: SQLConf): String = conf.getConf(THRIFTSERVER_HTTP_PATH)

  def maxMessageSize(conf: SQLConf): Int = conf.getConf(THRIFTSERVER_MAX_MESSAGE_SIZE)

  def thriftHttpMaxIdleTime(conf: SQLConf): Long =
    conf.getConf(THRIFTSERVER_THRIFT_HTTP_MAX_IDLE_TIME)

  def httpWorkerKeepaliveTime(conf: SQLConf): Long =
    conf.getConf(THRIFTSERVER_THRIFT_HTTP_WORKER_KEEPALIVE_TIME)

  def thriftHttpCookieAuthEnabled(conf: SQLConf): Boolean =
    conf.getConf(THRIFTSERVER_THRIFT_HTTP_COOKIE_AUTH_ENABLED)

  def thriftHttpCookieMaxAge(conf: SQLConf): Int =
    conf.getConf(THRIFTSERVER_THRIFT_HTTP_COOKIE_MAX_AGE).toInt

  def thriftHttpCookieDomain(conf: SQLConf): String =
    conf.getConf(THRIFTSERVER_THRIFT_HTTP_COOKIE_DOMAIN)

  def thriftHttpCookiePath(conf: SQLConf): String =
    conf.getConf(THRIFTSERVER_THRIFT_HTTP_COOKIE_PATH)

  def thriftHttpCookieIsSecure(conf: SQLConf): Boolean =
    conf.getConf(THRIFTSERVER_THRIFT_HTTP_COOKIE_IS_SECURE)

  def thriftHttpCookieIsHttpOnly(conf: SQLConf): Boolean =
    conf.getConf(THRIFTSERVER_THRIFT_HTTP_COOKIE_IS_HTTPONLY)

  def thriftPort(conf: SQLConf): Int = conf.getConf(THRIFTSERVER_THRIFT_PORT)

  def thriftSaslQop(conf: SQLConf): String = conf.getConf(THRIFTSERVER_THRIFT_SASL_QOP)

  def thriftMinThread(conf: SQLConf): Int = conf.getConf(THRIFTSERVER_THRIFT_MIN_WORKER_THREADS)

  def thriftMaxThread(conf: SQLConf): Int = conf.getConf(THRIFTSERVER_THRIFT_MAX_WORKER_THREADS)

  def thriftLoginBackoffSlotLength(conf: SQLConf): Int =
    conf.getConf(THRIFTSERVER_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH).toInt

  def thriftLoginTimeout(conf: SQLConf): Int =
    conf.getConf(THRIFTSERVER_THRIFT_LOGIN_TIMEOUT).intValue()

  def thriftWorkerKeepaliveTime(conf: SQLConf): Long =
    conf.getConf(THRIFTSERVER_THRIFT_WORKER_KEEPALIVE_TIME)

  def asyncExecThreads(conf: SQLConf): Int = conf.getConf(THRIFTSERVER_ASYNC_EXEC_THREADS)

  def asyncExecShutdownTimeout(conf: SQLConf): Long =
    conf.getConf(THRIFTSERVER_ASYNC_EXEC_SHUTDOWN_TIMEOUT)

  def asyncExecWaitQueueSize(conf: SQLConf): Int =
    conf.getConf(THRIFTSERVER_ASYNC_EXEC_WAIT_QUEUE_SIZE)

  def asyncExecKeepaliveTime(conf: SQLConf): Long =
    conf.getConf(THRIFTSERVER_ASYNC_EXEC_KEEPALIVE_TIME)

  def longPollingTimeout(conf: SQLConf): Long = conf.getConf(THRIFTSERVER_LONG_POLLING_TIMEOUT)

  def authentication(conf: SQLConf): String = conf.getConf(THRIFTSERVER_AUTHENTICATION)

  def allowUserSubstitution(conf: SQLConf): Boolean =
    conf.getConf(THRIFTSERVER_ALLOW_USER_SUBSTITUTION)

  def kerberosKeytab(conf: SQLConf): String = conf.getConf(THRIFTSERVER_KERBEROS_KEYTAB)

  def kerberosPrincipal(conf: SQLConf): String = conf.getConf(THRIFTSERVER_KERBEROS_PRINCIPAL)

  def spnegoKeytab(conf: SQLConf): String = conf.getConf(THRIFTSERVER_SPNEGO_KEYTAB)

  def spnegoPrincipal(conf: SQLConf): String = conf.getConf(THRIFTSERVER_SPNEGO_PRINCIPAL)

  def ldapUrl(conf: SQLConf): String = conf.getConf(THRIFTSERVER_PLAIN_LDAP_URL)

  def ldapBaseDN(conf: SQLConf): String = conf.getConf(THRIFTSERVER_PLAIN_LDAP_BASEDN)

  def ldapDomain(conf: SQLConf): String = conf.getConf(THRIFTSERVER_PLAIN_LDAP_DOMAIN)

  def customAuthenticationClass(conf: SQLConf): String =
    conf.getConf(THRIFTSERVER_CUSTOM_AUTHENTICATION_CLASS)

  def pamServices(conf: SQLConf): String = conf.getConf(THRIFTSERVER_PAM_SERVICES)

  def enableDoAs(conf: SQLConf): Boolean = conf.getConf(THRIFTSERVER_ENABLE_DOAS)

  def useSSL(conf: SQLConf): Boolean = conf.getConf(THRIFTSERVER_USE_SSL)

  def sslKeystorePath(conf: SQLConf): String = conf.getConf(THRIFTSERVER_SSL_KEYSTORE_PATH)

  def sslKeystorepassword(conf: SQLConf): String = conf.getConf(THRIFTSERVER_SSL_KEYSTORE_PASSWORD)

  def sslProtocolBlacklist(conf: SQLConf): String =
    conf.getConf(THRIFTSERVER_SSL_PROTOCOL_BLACKLIST)

  def sslProtocolWhitelist(conf: SQLConf): String = conf.getConf(THRIFTSERVER_BUILTIN_UDF_WHITELIST)

  def sessionCheckInterval(conf: SQLConf): Long = conf.getConf(THRIFTSERVER_SESSION_CHECK_INTERVAL)

  def idleSessionTimeout(conf: SQLConf): Long = conf.getConf(THRIFTSERVER_IDLE_SESSION_TIMEOUT)

  def idleSessionCheckOperation(conf: SQLConf): Boolean =
    conf.getConf(THRIFTSERVER_IDLE_SESSION_CHECK_OPERATION)

  def idleOperationTimeout(conf: SQLConf): Long = conf.getConf(THRIFTSERVER_IDLE_OPERATION_TIMEOUT)

  def loggingOperationEnabled(conf: SQLConf): Boolean =
    conf.getConf(THRIFTSERVER_LOGGING_OPERATION_ENABLE)

  def loggingOperationLocation(conf: SQLConf): String =
    conf.getConf(THRIFTSERVER_LOGGING_OPERATION_LOG_LOCATION)

  def loggingOperationLevel(conf: SQLConf): String =
    conf.getConf(THRIFTSERVER_LOGGING_OPERATION_LEVEL)

  def globalInitFileLocation(conf: SQLConf): String =
    conf.getConf(THRIFTSERVER_GLOABLE_INIT_FILE_LOCATION)
}

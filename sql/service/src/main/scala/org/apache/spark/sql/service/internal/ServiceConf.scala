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

package org.apache.spark.sql.service.internal

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the configuration options for Spark Thrift Server.
////////////////////////////////////////////////////////////////////////////////////////////////////

object ServiceConf {

  val THRIFTSERVER_ASYNC = ConfigBuilder("spark.sql.thriftServer.async")
    .doc("When set to true, Spark Thrift server executes SQL queries in an asynchronous way.")
    .booleanConf
    .createWithDefault(true)

  val THRIFTSERVER_TRANSPORT_MODE = ConfigBuilder("spark.sql.thriftserver.transport.mode")
    .internal()
    .doc("Transport mode of SparkThriftServer: 1. binary  2. http")
    .stringConf
    .createWithDefault("binary")

  val THRIFTSERVER_THRIFT_BIND_HOST = ConfigBuilder("spark.sql.thriftserver.thrift.bind.host")
    .internal()
    .doc("Bind host on which to run the SparkThriftServer Thrift service.")
    .stringConf
    .createWithDefault("")


  val THRIFTSERVER_HTTP_PORT = ConfigBuilder("spark.sql.thriftserver.http.port")
    .internal()
    .doc("Port number of SparkThriftServer Thrift interface " +
      "when spark.sql.thriftserver.transport.mode is 'http'.")
    .intConf
    .createWithDefault(10001)

  val THRIFTSERVER_HTTP_PATH = ConfigBuilder("spark.sql.thriftserver.thrift.http.path")
    .internal()
    .doc("Path component of URL endpoint when in HTTP mode.")
    .stringConf
    .createWithDefault("cliservice")

  val THRIFTSERVER_MAX_MESSAGE_SIZE =
    ConfigBuilder("spark.sql.thriftserver.max.message.size")
      .internal()
      .doc("Maximum message size in bytes a SS2 server will accept.")
      .intConf
      .createWithDefault(104857600)

  val THRIFTSERVER_THRIFT_HTTP_MAX_IDLE_TIME =
    ConfigBuilder("spark.sql.thriftserver.thrift.http.max.idle.time")
      .internal()
      .doc("Maximum idle time for a connection on the server when in HTTP mode.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(1800 * 1000)


  val THRIFTSERVER_THRIFT_HTTP_WORKER_KEEPALIVE_TIME =
    ConfigBuilder("spark.sql.thriftserver.thrift.http.worker.keepalive.time")
      .internal()
      .doc("Keepalive time for an idle http worker thread. " +
        "When the number of workers exceeds min workers, " +
        "excessive threads are killed after this time interval.")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(60)


  val THRIFTSERVER_THRIFT_HTTP_COOKIE_AUTH_ENABLED =
    ConfigBuilder("spark.sql.thriftserver.thrift.http.cookie.auth.enabled")
      .internal()
      .doc("When true, SparkThriftServer in HTTP transport mode," +
        " will use cookie based authentication mechanism.")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_MAX_AGE =
    ConfigBuilder("spark.sql.thriftserver.thrift.http.cookie.max.age")
      .internal()
      .doc("Maximum age in seconds for server side cookie used by SS2 in HTTP mode.")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(86400)

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_DOMAIN =
    ConfigBuilder("spark.sql.thriftserver.thrift.http.cookie.domain")
      .internal()
      .doc("Domain for the HS2 generated cookies")
      .stringConf
      .createWithDefault(null)

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_PATH =
    ConfigBuilder("spark.sql.thriftserver.thrift.http.cookie.path")
      .internal()
      .doc("Path for the SS2 generated cookies")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_IS_SECURE =
    ConfigBuilder("spark.sql.thriftserver.thrift.http.cookie.is.secure")
      .internal()
      .doc("Secure attribute of the SS2 generated cookie.")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_THRIFT_HTTP_COOKIE_IS_HTTPONLY =
    ConfigBuilder("spark.sql.thriftserver.thrift.http.cookie.is.httponly")
      .internal()
      .doc("HttpOnly attribute of the SS2 generated cookie.")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_THRIFT_PORT = ConfigBuilder("spark.sql.thriftserver.thrift.port")
    .internal()
    .doc("Port number of SparkThriftServer Thrift interface " +
      "when spark.sql.thriftserver.transport.mode is 'binary'.")
    .intConf
    .createWithDefault(10000)

  val THRIFTSERVER_THRIFT_SASL_QOP = ConfigBuilder("spark.sql.thriftserver.thrift.sasl.qop")
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
    .stringConf
    .createWithDefault("auth")

  val THRIFTSERVER_THRIFT_MIN_WORKER_THREADS =
    ConfigBuilder("spark.sql.thriftserver.thrift.min.worker.threads")
      .internal()
      .doc("Minimum number of Thrift worker threads")
      .intConf
      .createWithDefault(5)

  val THRIFTSERVER_THRIFT_MAX_WORKER_THREADS =
    ConfigBuilder("spark.sql.thriftserver.thrift.max.worker.threads")
      .internal()
      .doc("Maximum number of Thrift worker threads")
      .intConf
      .createWithDefault(500)


  val THRIFTSERVER_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH =
    ConfigBuilder("spark.sql.thriftserver.thrift.exponential.backoff.slot.length")
      .internal()
      .doc("Binary exponential backoff slot time for Thrift clients " +
        "during login to SparkThriftServer," +
        "for retries until hitting Thrift client timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(100)


  val THRIFTSERVER_THRIFT_LOGIN_TIMEOUT =
    ConfigBuilder("spark.sql.thriftserver.thrift.login.timeout")
      .internal()
      .doc("Timeout for Thrift clients during login to SparkThriftServer")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(20)


  val THRIFTSERVER_THRIFT_WORKER_KEEPALIVE_TIME =
    ConfigBuilder("spark.sql.thriftserver.thrift.worker.keepalive.time")
      .internal()
      .doc("Keepalive time (in seconds) for an idle worker thread. " +
        "When the number of workers exceeds min workers, " +
        "excessive threads are killed after this time interval.")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(60)

  val THRIFTSERVER_ASYNC_EXEC_THREADS =
    ConfigBuilder("spark.sql.thriftserver.async.exec.threads")
      .internal()
      .doc("Number of threads in the async thread pool for SparkThriftServer")
      .intConf
      .createWithDefault(100)

  val THRIFTSERVER_ASYNC_EXEC_SHUTDOWN_TIMEOUT =
    ConfigBuilder("spark.sql.thriftserver.async.exec.shutdown.timeout")
      .internal()
      .doc("How long SparkThriftServer shutdown will wait for async threads to terminate.")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(10)

  val THRIFTSERVER_ASYNC_EXEC_WAIT_QUEUE_SIZE =
    ConfigBuilder("spark.sql.thriftserver.async.exec.wait.queue.size")
      .internal()
      .doc("Size of the wait queue for async thread pool in SparkThriftServer." +
        "After hitting this limit, the async thread pool will reject new requests.")
      .intConf
      .createWithDefault(100)

  val THRIFTSERVER_ASYNC_EXEC_KEEPALIVE_TIME =
    ConfigBuilder("spark.sql.thriftserver.async.exec.keepalive.time")
      .internal()
      .doc("Time that an idle SparkThriftServer async thread (from the thread pool) " +
        "will wait for a new task to arrive before terminating")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(10)

  val THRIFTSERVER_LONG_POLLING_TIMEOUT =
    ConfigBuilder("spark.sql.thriftserver.long.polling.timeout")
      .internal()
      .doc("Time that SparkThriftServer will wait before responding to " +
        "asynchronous calls that use long polling")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(5000)

  val THRIFTSERVER_AUTHENTICATION =
    ConfigBuilder("spark.sql.thriftserver.authentication")
      .internal()
      .doc("Client authentication types." +
        "  NONE: no authentication check" +
        "  LDAP: LDAP/AD based authentication" +
        "  KERBEROS: Kerberos/GSSAPI authentication" +
        "  CUSTOM: Custom authentication provider" +
        "          (Use with property spark.sql.thriftserver.custom.authentication.class)" +
        "  PAM: Pluggable authentication module" +
        "  NOSASL:  Raw transport")
      .stringConf
      .createWithDefault("NONE")

  val THRIFTSERVER_ALLOW_USER_SUBSTITUTION =
    ConfigBuilder("spark.sql.thriftserver.allow.user.substitution")
      .internal()
      .doc("Allow alternate user to be specified " +
        "as part of SparkThriftServer open connection request.")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_KERBEROS_KEYTAB =
    ConfigBuilder("spark.sql.thriftserver.authentication.kerberos.keytab")
      .internal()
      .doc("Kerberos keytab file for server principal")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_KERBEROS_PRINCIPAL =
    ConfigBuilder("spark.sql.thriftserver.authentication.kerberos.principal")
      .internal()
      .doc("Kerberos server principal")
      .stringConf
      .createWithDefault("")


  val THRIFTSERVER_SPNEGO_KEYTAB =
    ConfigBuilder("spark.sql.thriftserver.authentication.spnego.keytab")
      .internal()
      .doc("keytab file for SPNego principal, optional," +
        "typical value would look like /etc/security/keytabs/spnego.service.keytab," +
        "This keytab would be used by SparkThriftServer when Kerberos security is enabled and " +
        "HTTP transport mode is used. " +
        "This needs to be set only if SPNEGO is to be used in authentication." +
        "SPNego authentication would be honored only " +
        "if valid spark.sql.thriftserver.authentication.spnego.principal" +
        "and hspark.sql.thriftserver.authentication.spnego.keytab are specified.")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SPNEGO_PRINCIPAL =
    ConfigBuilder("spark.sql.thriftserver.authentication.spnego.principal")
      .internal()
      .doc("SPNego service principal, optional, typical value " +
        "would look like HTTP/_HOST@EXAMPLE.COM" +
        "SPNego service principal would be used by SparkThriftServer" +
        " when Kerberos security is enabled and HTTP transport mode is used." +
        "This needs to be set only if SPNEGO is to be used in authentication.")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_PLAIN_LDAP_URL =
    ConfigBuilder("spark.sql.thriftserver.authentication.ldap.url")
      .internal()
      .doc("LDAP connection URL(s)," +
        "this value could contain URLs to mutiple LDAP servers instances for HA," +
        "each LDAP URL is separated by a SPACE character. URLs are used in the " +
        " order specified until a connection is successful.")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_PLAIN_LDAP_BASEDN =
    ConfigBuilder("spark.sql.thriftserver.authentication.ldap.baseDN")
      .internal()
      .doc("LDAP base DN")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_PLAIN_LDAP_DOMAIN =
    ConfigBuilder("spark.sql.thriftserver.authentication.ldap.Domain")
      .internal()
      .doc("")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_CUSTOM_AUTHENTICATION_CLASS =
    ConfigBuilder("spark.sql.thriftserver.custom.authentication.class")
      .internal()
      .doc("Custom authentication class. Used when property" +
        "'spark.sql.thriftserver.authentication' is set to 'CUSTOM'. Provided class" +
        "must be a proper implementation of the interface" +
        "org.apache.spark.sql.service.auth.PasswdAuthenticationProvider. SparkThriftServer" +
        "will call its Authenticate(user, passed) method to authenticate requests." +
        "The implementation may optionally implement Hadoop's" +
        "org.apache.hadoop.conf.Configurable class to grab Spark's Configuration object.")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_PAM_SERVICES =
    ConfigBuilder("spark.sql.thriftserver.authentication.pam.services")
      .internal()
      .doc("List of the underlying pam services that should be used when auth type is PAM" +
        "A file with the same name must exist in /etc/pam.d")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_ENABLE_DOAS =
    ConfigBuilder("spark.sql.thriftserver.enable.doAs")
      .internal()
      .doc("Setting this property to true will have SparkThriftServer execute" +
        "Spark operations as the user making the calls to it.")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_SESSION_HOOK =
    ConfigBuilder("spark.sql.thriftserver.session.hook")
      .internal()
      .doc("")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_USE_SSL =
    ConfigBuilder("spark.sql.thriftserver.use.SSL")
      .internal()
      .doc("Set this to true for using SSL encryption in SparkThriftServer.")
      .booleanConf
      .createWithDefault(false)

  val THRIFTSERVER_SSL_KEYSTORE_PATH =
    ConfigBuilder("spark.hadoop.spark.sql.thriftserver.ssl.keystore.path")
      .internal()
      .doc("SSL certificate keystore location.")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SSL_KEYSTORE_PASSWORD =
    ConfigBuilder("spark.hadoop.spark.sql.thriftserver.ssl.keystore.password")
      .internal()
      .doc("SSL certificate keystore password.")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SSL_PROTOCOL_BLACKLIST =
    ConfigBuilder("spark.sql.thriftserver.ssl.protocol.blacklist")
      .internal()
      .doc("SSL Versions to disable for all Hive Servers")
      .stringConf
      .createWithDefault("SSLv2,SSLv3")

  val THRIFTSERVER_BUILTIN_UDF_WHITELIST =
    ConfigBuilder("spark.sql.thriftserver.builtin.udf.whitelist")
      .internal()
      .doc("Comma separated list of builtin udf names allowed in queries." +
        "An empty whitelist allows all builtin udfs to be executed.  " +
        "The udf black list takes precedence over udf white list")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_BUILTIN_UDF_BLACKLIST =
    ConfigBuilder("spark.sql.thriftserver.builtin.udf.blacklist")
      .internal()
      .doc("Comma separated list of udfs names. These udfs will not be allowed in queries. " +
        "The udf black list takes precedence over udf white list")
      .stringConf
      .createWithDefault("")

  val THRIFTSERVER_SESSION_CHECK_INTERVAL =
    ConfigBuilder("spark.sql.thriftserver.session.check.interval")
      .internal()
      .doc("The check interval for session/operation timeout, " +
        "which can be disabled by setting to zero or negative value.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(6 * 3600 * 1000)

  val THRIFTSERVER_IDLE_SESSION_TIMEOUT =
    ConfigBuilder("spark.sql.thriftserver.idle.session.timeout")
      .internal()
      .doc("Session will be closed when it's not accessed for this duration, " +
        "which can be disabled by setting to zero or negative value.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(7 * 24 * 3600 * 1000)

  val THRIFTSERVER_IDLE_SESSION_CHECK_OPERATION =
    ConfigBuilder("spark.sql.thriftserver.idle.session.check.operation")
      .internal()
      .doc("Session will be considered to be idle only if there is no activity," +
        " and there is no pending operation. This setting takes effect " +
        "only if session idle timeout (spark.sql.thriftserver.idle.session.timeout) and " +
        "checking (spark.sql.thriftserver.session.check.interval) are enabled.")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_IDLE_OPERATION_TIMEOUT =
    ConfigBuilder("spark.sql.thriftserver.idle.operation.timeout")
      .internal()
      .doc("Operation will be closed when it's not accessed for this duration of time, " +
        "which can be disabled by setting to zero value." +
        "With positive value, it's checked for operations " +
        "     in terminal state only (FINISHED, CANCELED, CLOSED, ERROR)." +
        "With negative value, it's checked for all of the operations regardless of state.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(5 * 24 * 60 * 60 * 1000)

  val THRIFTSERVER_LOGGING_OPERATION_ENABLE =
    ConfigBuilder("spark.sql.thriftserver.logging.operation.enabled")
      .internal()
      .doc("When true, HS2 will save operation logs and make them available for clients")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_LOGGING_OPERATION_LOG_LOCATION =
    ConfigBuilder("spark.sql.thriftserver.logging.operation.log.location")
      .internal()
      .doc("Top level directory where operation logs are stored " +
        "if logging functionality is enabled")
      .stringConf
      .createWithDefault("${system:java.io.tmpdir}" + File.separator +
        "${system:user.name}" + File.separator + "operation_logs")

  val THRIFTSERVER_LOGGING_OPERATION_LEVEL =
    ConfigBuilder("spark.sql.thriftserver.logging.operation.level")
      .internal()
      .doc("HS2 operation logging mode available to clients to be set at session level." +
        "For this to work, spark.sql.thriftserver.logging.operation.enabled " +
        "should be set to true." +
        "   NONE: Ignore any logging" +
        "   EXECUTION: Log completion of tasks" +
        "   PERFORMANCE: Execution + Performance logs " +
        "   VERBOSE: All logs")
      .stringConf
      .createWithDefault("EXECUTION")

  val THRIFTSERVER_CLUSTER_DELEGATION_TOKEN_STORE_CLS =
    ConfigBuilder("spark.sql.thriftserver.cluster.delegation.token.store.class")
      .internal()
      .doc("The delegation token store implementation. " +
        "Set to org.apache.spark.sql.service.auth.thrift.ZooKeeperTokenStore " +
        "for load-balanced cluster.")
      .stringConf
      .createWithDefault("org.apache.spark.sql.service.auth.thrift.MemoryTokenStore")

  val THRIFTSERVER_VARIABLE_SUBSTITUTE =
    ConfigBuilder("spark.sql.thriftserver.variable.substitute")
      .internal()
      .doc("This enables substitution using syntax like ${var} ${system:var} and ${env:var}.")
      .booleanConf
      .createWithDefault(true)

  val THRIFTSERVER_VARIABLE_SUBSTITUTE_DEPTH =
    ConfigBuilder("spark.sql.thriftserver.variable.substitute.depth")
      .internal()
      .doc("The maximum replacements the substitution engine will do.")
      .intConf
      .createWithDefault(40)

  val THRIFTSERVER_GLOABLE_INIT_FILE_LOCATION =
    ConfigBuilder("spark.sql.thriftserver.global.init.file.location")
      .internal()
      .doc("Either the location of a SparkThriftServer global init file or a directory" +
        " containing a .sparkrc file. If the property is set, the value must be a valid path " +
        "to an init file or directory where the init file is located.")
      .stringConf
      .createWithDefault("${env:SPARK_CONF_DIR}")
}

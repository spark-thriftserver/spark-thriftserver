//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.spark.sql.service.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.spark.sql.service.auth.thrift.DelegationTokenIdentifier;
import org.apache.spark.sql.service.auth.thrift.DelegationTokenSelector;

public class Utils {
  public Utils() {
  }

  public static UserGroupInformation getUGI() throws LoginException, IOException {
    String doAs = System.getenv("HADOOP_USER_NAME");
    return doAs != null && doAs.length() > 0 ? UserGroupInformation.createProxyUser(doAs, UserGroupInformation.getLoginUser()) : UserGroupInformation.getCurrentUser();
  }

  public static String getTokenStrForm(String tokenSignature) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    TokenSelector<? extends TokenIdentifier> tokenSelector = new DelegationTokenSelector();
    Token<? extends TokenIdentifier> token = tokenSelector.selectToken(tokenSignature == null ? new Text() : new Text(tokenSignature), ugi.getTokens());
    return token != null ? token.encodeToUrlString() : null;
  }

  public static void setTokenStr(UserGroupInformation ugi, String tokenStr, String tokenService) throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    ugi.addToken(delegationToken);
  }

  public static String addServiceToToken(String tokenStr, String tokenService) throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    return delegationToken.encodeToUrlString();
  }

  private static Token<DelegationTokenIdentifier> createToken(String tokenStr, String tokenService) throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = new Token();
    delegationToken.decodeFromUrlString(tokenStr);
    delegationToken.setService(new Text(tokenService));
    return delegationToken;
  }

  public static void setZookeeperClientKerberosJaasConfig(String principal, String keyTabFile) throws IOException {
    String SASL_LOGIN_CONTEXT_NAME = "HiveZooKeeperClient";
    System.setProperty("zookeeper.sasl.clientconfig", "HiveZooKeeperClient");
    principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    Utils.JaasConfiguration jaasConf = new Utils.JaasConfiguration("HiveZooKeeperClient", principal, keyTabFile);
    Configuration.setConfiguration(jaasConf);
  }

  private static class JaasConfiguration extends Configuration {
    private final Configuration baseConfig = Configuration.getConfiguration();
    private final String loginContextName;
    private final String principal;
    private final String keyTabFile;

    public JaasConfiguration(String hiveLoginContextName, String principal, String keyTabFile) {
      this.loginContextName = hiveLoginContextName;
      this.principal = principal;
      this.keyTabFile = keyTabFile;
    }

    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (this.loginContextName.equals(appName)) {
        Map<String, String> krbOptions = new HashMap();
        krbOptions.put("doNotPrompt", "true");
        krbOptions.put("storeKey", "true");
        krbOptions.put("useKeyTab", "true");
        krbOptions.put("principal", this.principal);
        krbOptions.put("keyTab", this.keyTabFile);
        krbOptions.put("refreshKrb5Config", "true");
        AppConfigurationEntry hiveZooKeeperClientEntry = new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(), LoginModuleControlFlag.REQUIRED, krbOptions);
        return new AppConfigurationEntry[]{hiveZooKeeperClientEntry};
      } else {
        return this.baseConfig != null ? this.baseConfig.getAppConfigurationEntry(appName) : null;
      }
    }
  }
}

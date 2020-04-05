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

package org.apache.spark.sql.thriftserver.auth.thrift;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Map;
import javax.security.auth.callback.*;
import javax.security.sasl.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.thriftserver.auth.thrift.client.TUGIAssumingTransport;

/**
 * Functions that bridge Thrift's SASL transports to Hadoop's
 * SASL callback handlers and authentication classes.
 */
public class HadoopThriftAuthBridge {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopThriftAuthBridge.class);

  private static HadoopThriftAuthBridge instance;

  private HadoopThriftAuthBridge() {
  }

  public static HadoopThriftAuthBridge getInstance() {
    if (instance == null) {
      synchronized (HadoopThriftAuthBridge.class) {
        if (instance == null) {
          instance = new HadoopThriftAuthBridge();
        }
      }
    }
    return instance;
  }

  public Server createServer(String keytabFile, String principalConf) throws TTransportException {
    return new Server(keytabFile, principalConf);
  }

  public static class Server {
    public enum ServerMode {
      HIVESERVER2, METASTORE
    }

    protected final UserGroupInformation realUgi;
    protected DelegationTokenSecretManager secretManager;

    public Server() throws TTransportException {
      try {
        realUgi = UserGroupInformation.getCurrentUser();
      } catch (IOException ioe) {
        throw new TTransportException(ioe);
      }
    }

    /**
     * Create a server with a kerberos keytab/principal.
     */
    protected Server(String keytabFile, String principalConf)
        throws TTransportException {
      if (keytabFile == null || keytabFile.isEmpty()) {
        throw new TTransportException("No keytab specified");
      }
      if (principalConf == null || principalConf.isEmpty()) {
        throw new TTransportException("No principal specified");
      }

      // Login from the keytab
      String kerberosName;
      try {
        kerberosName =
            SecurityUtil.getServerPrincipal(principalConf, "0.0.0.0");
        UserGroupInformation.loginUserFromKeytab(
            kerberosName, keytabFile);
        realUgi = UserGroupInformation.getLoginUser();
        assert realUgi.isFromKeytab();
      } catch (IOException ioe) {
        throw new TTransportException(ioe);
      }
    }

    public void setSecretManager(DelegationTokenSecretManager secretManager) {
      this.secretManager = secretManager;
    }

    /**
     * Create a TTransportFactory that, upon connection of a client socket,
     * negotiates a Kerberized SASL transport. The resulting TTransportFactory
     * can be passed as both the input and output transport factory when
     * instantiating a TThreadPoolServer, for example.
     *
     * @param saslProps Map of SASL properties
     */

    public TTransportFactory createTransportFactory(Map<String, String> saslProps)
        throws TTransportException {

      TSaslServerTransport.Factory transFactory = createSaslServerTransportFactory(saslProps);

      return new TUGIAssumingTransportFactory(transFactory, realUgi);
    }

    /**
     * Create a TSaslServerTransport.Factory that, upon connection of a client
     * socket, negotiates a Kerberized SASL transport.
     *
     * @param saslProps Map of SASL properties
     */
    public TSaslServerTransport.Factory createSaslServerTransportFactory(
        Map<String, String> saslProps) throws TTransportException {
      // Parse out the kerberos principal, host, realm.
      String kerberosName = realUgi.getUserName();
      final String[] names = SaslRpcServer.splitKerberosName(kerberosName);
      if (names.length != 3) {
        throw new TTransportException("Kerberos principal should have 3 parts: " + kerberosName);
      }

      TSaslServerTransport.Factory transFactory = new TSaslServerTransport.Factory();
      transFactory.addServerDefinition(
          AuthMethod.KERBEROS.getMechanismName(),
          names[0], names[1],  // two parts of kerberos principal
          saslProps,
          new SaslRpcServer.SaslGssCallbackHandler());
      transFactory.addServerDefinition(AuthMethod.DIGEST.getMechanismName(),
          null, SaslRpcServer.SASL_DEFAULT_REALM,
          saslProps, new SaslDigestCallbackHandler(secretManager));

      return transFactory;
    }

    /**
     * Wrap a TProcessor to capture the client information like connecting userid, ip etc
     */

    public TProcessor wrapNonAssumingProcessor(TProcessor processor) {
      return new TUGIAssumingProcessor(processor, secretManager, false);
    }

    final ThreadLocal<InetAddress> remoteAddress =
        new ThreadLocal<InetAddress>() {

          @Override
          protected InetAddress initialValue() {
            return null;
          }
        };

    public InetAddress getRemoteAddress() {
      return remoteAddress.get();
    }

    final ThreadLocal<AuthenticationMethod> authenticationMethod =
        new ThreadLocal<AuthenticationMethod>() {

          @Override
          protected AuthenticationMethod initialValue() {
            return AuthenticationMethod.TOKEN;
          }
        };

    private static ThreadLocal<String> remoteUser = new ThreadLocal<String>() {

      @Override
      protected String initialValue() {
        return null;
      }
    };


    public String getRemoteUser() {
      return remoteUser.get();
    }

    private final ThreadLocal<String> userAuthMechanism =
        new ThreadLocal<String>() {

          @Override
          protected String initialValue() {
            return AuthMethod.KERBEROS.getMechanismName();
          }
        };

    public String getUserAuthMechanism() {
      return userAuthMechanism.get();
    }

    /** CallbackHandler for SASL DIGEST-MD5 mechanism */
    // This code is pretty much completely based on Hadoop's
    // SaslRpcServer.SaslDigestCallbackHandler - the only reason we could not
    // use that Hadoop class as-is was because it needs a Server.Connection object
    // which is relevant in hadoop rpc but not here in the metastore - so the
    // code below does not deal with the Connection Server.object.
    static class SaslDigestCallbackHandler implements CallbackHandler {
      private final DelegationTokenSecretManager secretManager;

      SaslDigestCallbackHandler(
          DelegationTokenSecretManager secretManager) {
        this.secretManager = secretManager;
      }

      private char[] getPassword(DelegationTokenIdentifier tokenid) throws InvalidToken {
        return encodePassword(secretManager.retrievePassword(tokenid));
      }

      private char[] encodePassword(byte[] password) {
        return new String(Base64.encodeBase64(password)).toCharArray();
      }

      /** {@inheritDoc} */

      @Override
      public void handle(Callback[] callbacks) throws InvalidToken,
          UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        AuthorizeCallback ac = null;
        for (Callback callback : callbacks) {
          if (callback instanceof AuthorizeCallback) {
            ac = (AuthorizeCallback) callback;
          } else if (callback instanceof NameCallback) {
            nc = (NameCallback) callback;
          } else if (callback instanceof PasswordCallback) {
            pc = (PasswordCallback) callback;
          } else if (callback instanceof RealmCallback) {
            continue; // realm is ignored
          } else {
            throw new UnsupportedCallbackException(callback,
                "Unrecognized SASL DIGEST-MD5 Callback");
          }
        }
        if (pc != null) {
          DelegationTokenIdentifier tokenIdentifier =
              SaslRpcServer.getIdentifier(nc.getDefaultName(), secretManager);
          char[] password = getPassword(tokenIdentifier);

          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server DIGEST-MD5 callback: setting password "
                + "for client: " + tokenIdentifier.getUser());
          }
          pc.setPassword(password);
        }
        if (ac != null) {
          String authid = ac.getAuthenticationID();
          String authzid = ac.getAuthorizationID();
          if (authid.equals(authzid)) {
            ac.setAuthorized(true);
          } else {
            ac.setAuthorized(false);
          }
          if (ac.isAuthorized()) {
            if (LOG.isDebugEnabled()) {
              String username =
                  SaslRpcServer.getIdentifier(authzid, secretManager).getUser().getUserName();
              LOG.debug("SASL server DIGEST-MD5 callback: setting "
                  + "canonicalized client ID: " + username);
            }
            ac.setAuthorizedID(authzid);
          }
        }
      }
    }

    /**
     * Processor that pulls the SaslServer object out of the transport, and
     * assumes the remote user's UGI before calling through to the original
     * processor.
     *
     * This is used on the server side to set the UGI for each specific call.
     */
    protected class TUGIAssumingProcessor implements TProcessor {
      final TProcessor wrapped;
      DelegationTokenSecretManager secretManager;
      boolean useProxy;

      TUGIAssumingProcessor(TProcessor wrapped, DelegationTokenSecretManager secretManager,
                            boolean useProxy) {
        this.wrapped = wrapped;
        this.secretManager = secretManager;
        this.useProxy = useProxy;
      }


      @Override
      public boolean process(final TProtocol inProt, final TProtocol outProt) throws TException {
        TTransport trans = inProt.getTransport();
        if (!(trans instanceof TSaslServerTransport)) {
          throw new TException("Unexpected non-SASL transport " + trans.getClass());
        }
        TSaslServerTransport saslTrans = (TSaslServerTransport) trans;
        SaslServer saslServer = saslTrans.getSaslServer();
        String authId = saslServer.getAuthorizationID();
        LOG.debug("AUTH ID ======>" + authId);
        String endUser = authId;

        Socket socket = ((TSocket) (saslTrans.getUnderlyingTransport())).getSocket();
        remoteAddress.set(socket.getInetAddress());

        String mechanismName = saslServer.getMechanismName();
        userAuthMechanism.set(mechanismName);
        if (AuthMethod.PLAIN.getMechanismName().equalsIgnoreCase(mechanismName)) {
          remoteUser.set(endUser);
          return wrapped.process(inProt, outProt);
        }

        authenticationMethod.set(AuthenticationMethod.KERBEROS);
        if (AuthMethod.TOKEN.getMechanismName().equalsIgnoreCase(mechanismName)) {
          try {
            TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authId,
                secretManager);
            endUser = tokenId.getUser().getUserName();
            authenticationMethod.set(AuthenticationMethod.TOKEN);
          } catch (InvalidToken e) {
            throw new TException(e.getMessage());
          }
        }

        UserGroupInformation clientUgi = null;
        try {
          if (useProxy) {
            clientUgi = UserGroupInformation.createProxyUser(
                endUser, UserGroupInformation.getLoginUser());
            remoteUser.set(clientUgi.getShortUserName());
            LOG.debug("Set remoteUser :" + remoteUser.get());
            return clientUgi.doAs(new PrivilegedExceptionAction<Boolean>() {

              @Override
              public Boolean run() {
                try {
                  return wrapped.process(inProt, outProt);
                } catch (TException te) {
                  throw new RuntimeException(te);
                }
              }
            });
          } else {
            // use the short user name for the request
            UserGroupInformation endUserUgi = UserGroupInformation.createRemoteUser(endUser);
            remoteUser.set(endUserUgi.getShortUserName());
            LOG.debug("Set remoteUser :" + remoteUser.get() + ", from endUser :" + endUser);
            return wrapped.process(inProt, outProt);
          }
        } catch (RuntimeException rte) {
          if (rte.getCause() instanceof TException) {
            throw (TException) rte.getCause();
          }
          throw rte;
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie); // unexpected!
        } catch (IOException ioe) {
          throw new RuntimeException(ioe); // unexpected!
        } finally {
          if (clientUgi != null) {
            try {
              FileSystem.closeAllForUGI(clientUgi);
            } catch (IOException exception) {
              LOG.error("Could not clean up file-system handles for UGI: " + clientUgi, exception);
            }
          }
        }
      }
    }

    /**
     * A TransportFactory that wraps another one, but assumes a specified UGI
     * before calling through.
     *
     * This is used on the server side to assume the server's Principal when accepting
     * clients.
     */
    static class TUGIAssumingTransportFactory extends TTransportFactory {
      private final UserGroupInformation ugi;
      private final TTransportFactory wrapped;

      TUGIAssumingTransportFactory(TTransportFactory wrapped, UserGroupInformation ugi) {
        assert wrapped != null;
        assert ugi != null;
        this.wrapped = wrapped;
        this.ugi = ugi;
      }


      @Override
      public TTransport getTransport(final TTransport trans) {
        return ugi.doAs(new PrivilegedAction<TTransport>() {
          @Override
          public TTransport run() {
            return wrapped.getTransport(trans);
          }
        });
      }
    }
  }
}
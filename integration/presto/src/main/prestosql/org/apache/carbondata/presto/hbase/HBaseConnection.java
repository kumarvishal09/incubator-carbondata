/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.presto.hbase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.carbondata.presto.hbase.security.HBaseKerberosAuthentication;
import org.apache.carbondata.presto.impl.CarbonTableConfig;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * HBaseConnection
 */
public class HBaseConnection {
  private static final Logger LOG = Logger.get(HBaseConnection.class);
  // if zookeeper.sasl.client.config not set，then ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME is Client_new，
  // otherwise the kerberos on zookeeper will fail
  private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client_new";
  /**
   * hbase config
   */
  protected CarbonTableConfig hbaseConfig;
  /**
   * hbase Connection
   */
  protected volatile Connection conn;
  /**
   * hbase admin
   */
  protected HBaseAdmin hbaseAdmin;
  /**
   * Configuration
   */
  protected Configuration cfg;
  /**
   * catalogFile
   */
  protected String catalogFile;

  private UserGroupInformation ugi;

  /**
   * constructor
   *
   * @param config HBaseConfig
   */
  @Inject public HBaseConnection(CarbonTableConfig config) {
    this.hbaseConfig = config;
    authenticate();
  }

  private void authenticate() {
    cfg = HBaseConfiguration.create();
    cfg.set("hbase.client.retries.number", hbaseConfig.getRetryNumber() + "");
    cfg.set("hbase.client.pause", hbaseConfig.getPauseTime() + "");

    if ("KERBEROS".equals(hbaseConfig.getKerberos())) {
      try {
        if (!isFileExist(hbaseConfig.getHbaseSitePath())) {
          throw new FileNotFoundException(hbaseConfig.getHbaseSitePath());
        }
        cfg.addResource(new Path(hbaseConfig.getHbaseSitePath()));
        if (hbaseConfig.getJaasConfPath() != null && !hbaseConfig.getJaasConfPath().isEmpty()) {
          System.setProperty("java.security.auth.login.config", hbaseConfig.getJaasConfPath());
        }
        if (hbaseConfig.getKrb5ConfPath() != null && !hbaseConfig.getKrb5ConfPath().isEmpty()) {
          System.setProperty("java.security.krb5.conf", hbaseConfig.getKrb5ConfPath());
        }
        if (hbaseConfig.isRpcProtectionEnable()) {
          cfg.set("hbase.rpc.protection", "privacy");
        }

        cfg.set("username.client.keytab.file", hbaseConfig.getUserKeytabPath());
        cfg.set("username.client.kerberos.principal", hbaseConfig.getPrincipalUsername());
        cfg.set("hadoop.security.authentication", "Kerberos");
        cfg.set("hbase.security.authentication", "Kerberos");

        String userName = hbaseConfig.getPrincipalUsername();
        String userKeytabFile = hbaseConfig.getUserKeytabPath();
        String krb5File = System.getProperty("java.security.krb5.conf");
        HBaseKerberosAuthentication
            .setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
        ugi = HBaseKerberosAuthentication
            .authenticateAndReturnUGI(userName, userKeytabFile, krb5File, cfg);
      } catch (IOException e) {
        LOG.error(e);
        LOG.error("auth failed...cause by %s", e);
      }
    }
  }

  /**
   * initConnection
   */
  public void initConnection() {
    cfg.set("hbase.zookeeper.quorum", hbaseConfig.getZkQuorum());
    cfg.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZkClientPort());
    try {
      init();
    } catch (IOException e) {
      LOG.error("initConnection failed...cause by %s", e);
      conn = null;
    }
  }

  /**
   * check file exist
   *
   * @param path file path
   * @return true/false
   */
  public static boolean isFileExist(String path) {
    File file = new File(path);
    return file.exists();
  }

  /**
   * init
   *
   * @throws IOException IOException
   */
  public void init() throws IOException {
    if ("KERBEROS".equals(hbaseConfig.getKerberos())) {
      ugi.doAs(new PrivilegedAction<Connection>() {
        /**
         * @return Object
         */
        public Connection run() {
          try {
            conn = ConnectionFactory.createConnection(cfg);
          } catch (IOException e) {
            LOG.error("[safemode] create fi hbase connector failed...cause by : %s", e);
          }
          return conn;
        }
      });
    } else {
      try {
        conn = ConnectionFactory.createConnection(cfg);
      } catch (IOException e) {
        LOG.error("create fi hbase connection failed...cause by: %s", e);
        conn = null;
      }
    }
  }

  /**
   * getConn
   *
   * @return Connection
   */
  public Connection getConn() {
    if (conn == null) {
      initConnection();
    }

    return conn;
  }

  /**
   * getHbaseAdmin
   *
   * @return HBaseAdmin
   */
  public HBaseAdmin getHbaseAdmin() {
    try {
      Admin admin = this.getConn().getAdmin();
      if (admin instanceof HBaseAdmin) {
        hbaseAdmin = (HBaseAdmin) admin;
      }
    } catch (IOException e) {
      LOG.error("getHbaseAdmin failed... cause by %s", e);
      hbaseAdmin = null;
    }
    return hbaseAdmin;
  }
}

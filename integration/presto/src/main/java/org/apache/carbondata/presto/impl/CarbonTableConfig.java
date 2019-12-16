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

package org.apache.carbondata.presto.impl;

import io.airlift.configuration.Config;

/**
 * Configuration read from etc/catalog/carbondata.properties
 */
public class CarbonTableConfig {

  //read from config
  private String unsafeMemoryInMb;
  private String enableUnsafeInQueryExecution;
  private String enableUnsafeColumnPage;
  private String enableUnsafeSort;
  private String enableQueryStatistics;
  private String batchSize;
  private String s3A_acesssKey;
  private String s3A_secretKey;
  private String s3_acesssKey;
  private String s3_secretKey;
  private String s3N_acesssKey;
  private String s3N_secretKey;
  private String endPoint;
  private String pushRowFilter;
  private static final int RETRY_NUMBER = 3;
  private static final int PAUSE_TIME = 100;
  private int retryNumber = RETRY_NUMBER;
  private int pauseTime = PAUSE_TIME;
  private boolean isRpcProtectionEnable; // Whether to enable hbase data communication encryption, default is false
  private String zkQuorum;
  private String zkClientPort;
  private String metastoreType;
  private String metastoreUrl;
  private String defaultValue = "NULL";
  private String jaasConfPath; // java.security.auth.login.config: jaas.conf
  private String coreSitePath; // core-site.xml file path
  private String hdfsSitePath; // hdfs-site.xml file path
  private String hbaseSitePath; // hbase-site.xml file path
  private String krb5ConfPath; // java.security.krb5.conf: krb5.conf
  private String userKeytabPath; // user.keytab file path
  private String principalUsername; // principal username
  private String kerberos;

  /**
   * Property to send load model from coordinator to worker in presto. This is internal constant
   * and not exposed to user.
   */
  public static final String CARBON_PRESTO_LOAD_MODEL = "carbondata.presto.encoded.loadmodel";

  public String getUnsafeMemoryInMb() {
    return unsafeMemoryInMb;
  }

  @Config("carbon.unsafe.working.memory.in.mb")
  public CarbonTableConfig setUnsafeMemoryInMb(String unsafeMemoryInMb) {
    this.unsafeMemoryInMb = unsafeMemoryInMb;
    return this;
  }

  public String getEnableUnsafeInQueryExecution() {
    return enableUnsafeInQueryExecution;
  }

  @Config("enable.unsafe.in.query.processing")
  public CarbonTableConfig setEnableUnsafeInQueryExecution(String enableUnsafeInQueryExecution) {
    this.enableUnsafeInQueryExecution = enableUnsafeInQueryExecution;
    return this;
  }

  public String getEnableUnsafeColumnPage() {
    return enableUnsafeColumnPage;
  }

  @Config("enable.unsafe.columnpage")
  public CarbonTableConfig setEnableUnsafeColumnPage(String enableUnsafeColumnPage) {
    this.enableUnsafeColumnPage = enableUnsafeColumnPage;
    return this;
  }

  public String getEnableUnsafeSort() {
    return enableUnsafeSort;
  }

  @Config("enable.unsafe.sort")
  public CarbonTableConfig setEnableUnsafeSort(String enableUnsafeSort) {
    this.enableUnsafeSort = enableUnsafeSort;
    return this;
  }

  public String getEnableQueryStatistics() {
    return enableQueryStatistics;
  }

  @Config("enable.query.statistics")
  public CarbonTableConfig setEnableQueryStatistics(String enableQueryStatistics) {
    this.enableQueryStatistics = enableQueryStatistics;
    return this;
  }

  public String getBatchSize() {
    return batchSize;
  }

  @Config("query.vector.batchSize")
  public CarbonTableConfig setBatchSize(String batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public String getS3A_AcesssKey() {
    return s3A_acesssKey;
  }

  public String getS3A_SecretKey() {
    return s3A_secretKey;
  }

  public String getS3_AcesssKey() {
    return s3_acesssKey;
  }

  public String getS3_SecretKey() {
    return s3_secretKey;
  }

  public String getS3N_AcesssKey() {
    return s3N_acesssKey;
  }

  public String getS3N_SecretKey() {
    return s3N_secretKey;
  }

  public String getS3EndPoint() {
    return endPoint;
  }

  @Config("fs.s3a.access.key")
  public CarbonTableConfig setS3A_AcesssKey(String s3A_acesssKey) {
    this.s3A_acesssKey = s3A_acesssKey;
    return this;
  }

  @Config("fs.s3a.secret.key")
  public CarbonTableConfig setS3A_SecretKey(String s3A_secretKey) {
    this.s3A_secretKey = s3A_secretKey;
    return this;
  }

  @Config("fs.s3.awsAccessKeyId")
  public CarbonTableConfig setS3_AcesssKey(String s3_acesssKey) {
    this.s3_acesssKey = s3_acesssKey;
    return this;
  }

  @Config("fs.s3.awsSecretAccessKey")
  public CarbonTableConfig setS3_SecretKey(String s3_secretKey) {
    this.s3_secretKey = s3_secretKey;
    return this;
  }

  @Config("fs.s3n.awsAccessKeyId")
  public CarbonTableConfig setS3N_AcesssKey(String s3N_acesssKey) {
    this.s3N_acesssKey = s3N_acesssKey;
    return this;
  }

  @Config("fs.s3.awsSecretAccessKey")
  public CarbonTableConfig setS3N_SecretKey(String s3N_secretKey) {
    this.s3N_secretKey = s3N_secretKey;
    return this;
  }

  @Config("fs.s3a.endpoint")
  public CarbonTableConfig setS3EndPoint(String endPoint) {
    this.endPoint = endPoint;
    return this;
  }

  public String getPushRowFilter() {
    return pushRowFilter;
  }

  @Config("carbon.push.rowfilters.for.vector")
  public void setPushRowFilter(String pushRowFilter) {
    this.pushRowFilter = pushRowFilter;
  }

  public int getRetryNumber()
  {
    return retryNumber;
  }

  @Config("hbase.client.retries.number")
  public void setRetryNumber(int retryNumber)
  {
    this.retryNumber = retryNumber;
  }

  public int getPauseTime()
  {
    return pauseTime;
  }

  @Config("hbase.client.pause.time")
  public void setPauseTime(int pauseTime)
  {
    this.pauseTime = pauseTime;
  }

  public boolean isRpcProtectionEnable()
  {
    return isRpcProtectionEnable;
  }

  @Config("hbase.rpc.protection.enable")
  public void setRpcProtectionEnable(boolean rpcProtectionEnable)
  {
    isRpcProtectionEnable = rpcProtectionEnable;
  }

  public String getJaasConfPath()
  {
    return jaasConfPath;
  }

  @Config("hbase.jaas.conf.path")
  public void setJaasConfPath(String jaasConfPath)
  {
    this.jaasConfPath = jaasConfPath;
  }

  public String getCoreSitePath()
  {
    return coreSitePath;
  }

  @Config("hbase.core.site.path")
  public void setCoreSitePath(String coreSitePath)
  {
    this.coreSitePath = coreSitePath;
  }

  public String getHdfsSitePath()
  {
    return hdfsSitePath;
  }

  @Config("hbase.hdfs.site.path")
  public void setHdfsSitePath(String hdfsSitePath)
  {
    this.hdfsSitePath = hdfsSitePath;
  }

  public String getHbaseSitePath()
  {
    return hbaseSitePath;
  }

  @Config("hbase.hbase.site.path")
  public void setHbaseSitePath(String hbaseSitePath)
  {
    this.hbaseSitePath = hbaseSitePath;
  }

  public String getKrb5ConfPath()
  {
    return krb5ConfPath;
  }

  @Config("hbase.krb5.conf.path")
  public void setKrb5ConfPath(String krb5ConfPath)
  {
    this.krb5ConfPath = krb5ConfPath;
  }

  public String getUserKeytabPath()
  {
    return userKeytabPath;
  }

  @Config("hbase.kerberos.keytab")
  public void setUserKeytabPath(String userKeytabPath)
  {
    this.userKeytabPath = userKeytabPath;
  }

  public String getPrincipalUsername()
  {
    return principalUsername;
  }

  @Config("hbase.kerberos.principal")
  public void setPrincipalUsername(String principalUsername)
  {
    this.principalUsername = principalUsername;
  }

  public String getZkQuorum()
  {
    return zkQuorum;
  }

  @Config("hbase.zookeeper.quorum")
  public void setZkQuorum(String zkQuorum)
  {
    this.zkQuorum = zkQuorum;
  }

  public String getZkClientPort()
  {
    return zkClientPort;
  }

  @Config("hbase.zookeeper.property.clientPort")
  public void setZkClientPort(String zkClientPort)
  {
    this.zkClientPort = zkClientPort;
  }

  public String getMetastoreType()
  {
    return metastoreType;
  }

  @Config("hbase.metastore.type")
  public void setMetastoreType(String metastoreType)
  {
    this.metastoreType = metastoreType;
  }

  public String getMetastoreUrl()
  {
    return metastoreUrl;
  }

  @Config("hbase.metastore.uri")
  public void setMetastoreUrl(String metastoreUrl)
  {
    this.metastoreUrl = metastoreUrl;
  }

  public String getKerberos()
  {
    return kerberos;
  }

  @Config("hbase.authentication.type")
  public void setKerberos(String kerberos)
  {
    this.kerberos = kerberos;
  }

  public String getDefaultValue()
  {
    return defaultValue;
  }

  @Config("hbase.default.value")
  public void setDefaultValue(String defaultValue)
  {
    this.defaultValue = defaultValue;
  }
}

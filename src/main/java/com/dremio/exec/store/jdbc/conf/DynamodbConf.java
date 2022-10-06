/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
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
package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.validation.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

/**
 * Configuration for SQLite sources.
 */
@SourceType(value = "DynamoDB", label = "DynamoDB", uiConfig = "dynamoarp-layout.json", externalQuerySupported = true)
public class DynamodbConf extends AbstractArpConf<DynamodbConf> {
  private static final String ARP_FILENAME = "arp/implementation/dynamodb-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "com.simba.dynamodb.jdbc.Driver";

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "JDBC Connection String Example:\n" +
          "jdbc:dynamodb:Host=dynamodb.us-west-1.amazonaws.com;" +
          "Region=us-west-1;" +
          "AccessKey=ABCABCABC123ABCABC45;" +
          "SecretKey=abCD+E1f2Gxhi3J4klmN/OP5QrSTuvwXYzabcdEF")
  public String jdbcstring;


  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String jdbcstring = checkNotNull(this.jdbcstring, "Missing JDBC Conenction String.");

    return String.format("%s;", jdbcstring);
  }

  @Tag(2)
  @DisplayMetadata(label = "Maximum idle connections")
  @NotMetadataImpacting
  public int maxIdleConns = 8;

  @Tag(3)
  @DisplayMetadata(label = "Connection idle time (s)")
  @NotMetadataImpacting
  public int idleTimeSec = 60;

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(
          JdbcPluginConfig.Builder configBuilder,
          CredentialsService credentialsService,
          OptionManager optionManager
  ) {
     return configBuilder.withDialect(getDialect())
              .withDialect(getDialect())
              .withDatasourceFactory(this::newDataSource)
              .clearHiddenSchemas()
              .addHiddenSchema("SYSTEM")
              .build();
  }

  private CloseableDataSource newDataSource() {
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
            toJdbcConnectionString(), null, null, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE,
            maxIdleConns, idleTimeSec);
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}

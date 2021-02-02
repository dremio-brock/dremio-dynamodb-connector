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

import org.hibernate.validator.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

/**
 * Configuration for SQLite sources.
 */
@SourceType(value = "DynamoDB", label = "DynamoDB", uiConfig = "dynamoarp-layout.json")
public class DynamodbConf extends AbstractArpConf<DynamodbConf> {
  private static final String ARP_FILENAME = "arp/implementation/dynamodb-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "com.simba.dynamodb.jdbc.Driver";

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Host")
  public String host;

  @NotBlank
  @Tag(2)
  @DisplayMetadata(label = "Access Key")
  public String accesskey;

  @NotBlank
  @Tag(3)
  @DisplayMetadata(label = "Secrect Key")
  public String secrectkey;

  @NotBlank
  @Tag(4)
  @DisplayMetadata(label = "Region")
  public String region;

  @Tag(5)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Grant External Query access (External Query allows creation of VDS from a Sybase query. Learn more here: https://docs.dremio.com/data-sources/external-queries.html#enabling-external-queries)")
  public boolean enableExternalQuery = false;

  //@NotBlank
  //@Tag(5)
  //@DisplayMetadata(label = "Local Metadata File")
  //public String localmetadatafile;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String host = checkNotNull(this.host, "Missing Host.");

    final String accesskey = checkNotNull(this.accesskey, "Missing Access Key.");

    final String secrectkey = checkNotNull(this.secrectkey, "Missing Secrect Key.");

    final String region = checkNotNull(this.region, "Missing Region.");

    //final String localmetadatafile = checkNotNull(this.localmetadatafile, "Missing Local Metadata File.");

    //return String.format("jdbc:dynamodb:Host=%s;AccessKey=%s;SecretKey=%s;Region=%s;LocalMetadataFile=%s", host, accesskey, secrectkey, region, localmetadatafile);

    return String.format("jdbc:dynamodb:Host=%s;AccessKey=%s;SecretKey=%s;Region=%s;", host, accesskey, secrectkey, region);
  }

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(JdbcPluginConfig.Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
         return configBuilder.withDialect(getDialect())
        .withDialect(getDialect())
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        .addHiddenSchema("SYSTEM")
        .withAllowExternalQuery(enableExternalQuery)
        .build();
  }

  private CloseableDataSource newDataSource() {
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
      toJdbcConnectionString(), null, null, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
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

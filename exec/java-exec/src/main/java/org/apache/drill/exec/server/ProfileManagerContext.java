/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server;

import java.io.IOException;
//import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.store.LocalPersistentStore;
import org.apache.drill.exec.store.sys.store.provider.ZookeeperPersistentStoreProvider;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds context for Profile Manager and is used by LocalPersistentStore as well
 */
public class ProfileManagerContext {
  private static final Logger logger = LoggerFactory.getLogger(ProfileManagerContext.class);
  private static final String ARCHIVE_LOCATION = "archived";

  private DrillConfig drillConfig;
  private LocalPersistentStore<QueryProfile> completedProfileStore;
  private int archivalThreshold;
  private int archivalRate;
  private Path archivalPath;
  private long archivalInterval;
  private DrillFileSystem fs;
  private ZKClusterCoordinator zkCoord;
  private final boolean useZkCoordinatedManagement;

  public ProfileManagerContext(ClusterCoordinator coord, DrillbitContext drillbitContext) throws StoreException, IOException {
    this.drillConfig = drillbitContext.getConfig();
    this.completedProfileStore = (LocalPersistentStore<QueryProfile>) drillbitContext.getProfileStoreContext().getCompletedProfileStore();
    try {
      this.fs = inferFileSystem(drillConfig);
    } catch (IOException ex) {
      throw new StoreException("Unable to get filesystem", ex);
    }

    //Use Zookeeper for coordinated management
    if (this.useZkCoordinatedManagement = isFileSysDistributed(fs)) {
      this.zkCoord = (ZKClusterCoordinator) coord;
    }

    this.archivalPath = new Path(getCompletedProfileStore().getBasePath(), ARCHIVE_LOCATION);
    this.archivalInterval = drillConfig.getInt(ExecConstants.PROFILES_STORE_ARCHIVE_INTERVAL);
    //Ensure not exceed MAX_HTTP_PROFILES
    final int maxHttpProfiles = drillConfig.getInt(ExecConstants.HTTP_MAX_PROFILES);
    this.archivalThreshold = Math.max(maxHttpProfiles, drillConfig.getInt(ExecConstants.PROFILES_STORE_CAPACITY));
    this.archivalRate = drillConfig.getInt(ExecConstants.PROFILES_STORE_ARCHIVE_RATE);
  }

  public DrillConfig getDrillConfig() {
    return drillConfig;
  }

  public DrillFileSystem getFileSystem() {
    return fs;
  }

  public ZKClusterCoordinator getZkCoord() {
    return zkCoord;
  }

  public boolean useZkCoordinatedManagement() {
    return useZkCoordinatedManagement;
  }

  public LocalPersistentStore<QueryProfile> getCompletedProfileStore() {
    return completedProfileStore;
  }

  public int getArchivalThreshold() {
    return archivalThreshold;
  }

  public int getArchivalRate() {
    return archivalRate;
  }

  public Path getArchivalPath() {
    return archivalPath;
  }

  public long getArchivalInterval() {
    return archivalInterval;
  }

  private DrillFileSystem inferFileSystem(DrillConfig drillConfig) throws IOException {
    boolean hasZkBlobRoot = drillConfig.hasPath(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT);
    final Path blobRoot = hasZkBlobRoot ?
        new org.apache.hadoop.fs.Path(drillConfig.getString(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT)) :
          LocalPersistentStore.getLogDir();

    return LocalPersistentStore.getFileSystem(drillConfig, blobRoot);
  }

  private boolean isFileSysDistributed(DrillFileSystem fileSystem) {
    final List<String> supportedFS = drillConfig.getStringList(ExecConstants.PROFILES_STORE_ARCHIVE_SUPPORTED_FS);
    if (supportedFS.contains(fileSystem.getScheme())) {
      return true;
    }
    return false;
  }

}

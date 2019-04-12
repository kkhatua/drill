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
package org.apache.drill.exec.server.profile;

import static org.apache.drill.exec.ExecConstants.DRILL_SYS_FILE_SUFFIX;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DistributedSemaphore.DistributedLease;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZkDistributedSemaphore;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.store.DrillSysFilePathFilter;
import org.apache.drill.exec.store.sys.store.LocalPersistentStore;
import org.apache.drill.exec.store.sys.store.ProfileSet;
import org.apache.drill.exec.store.sys.store.provider.ZookeeperPersistentStoreProvider;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage profiles by archiving
 */
public class ProfileIndexer {
  private static final Logger logger = LoggerFactory.getLogger(ProfileIndexer.class);
  private static final String lockPathString = "/profileManager";
  private static final int DRILL_SYS_FILE_EXT_SIZE = DRILL_SYS_FILE_SUFFIX.length();

  private final ZKClusterCoordinator zkCoord;
  private final DrillFileSystem fs;
  private final Path basePath;
  private final ProfileSet profiles;
  private final int indexingRate;
  private final PathFilter sysFileSuffixFilter;
  private SimpleDateFormat indexedPathFormat;
  private final boolean useZkCoordinatedManagement;
  private DrillConfig drillConfig;

  private PersistentStoreConfig<QueryProfile> pStoreConfig;
  private LocalPersistentStore<QueryProfile> completedProfileStore;
  private Stopwatch indexWatch;


  /**
   * ProfileIndexer
   */
  public ProfileIndexer(ClusterCoordinator coord, DrillbitContext context) throws StoreException, IOException {
    drillConfig = context.getConfig();

    // FileSystem
    try {
      this.fs = inferFileSystem(drillConfig);
    } catch (IOException ex) {
      throw new StoreException("Unable to get filesystem", ex);
    }

    //Use Zookeeper for coordinated management
    if (this.useZkCoordinatedManagement = isFileSysDistributed(fs)) {
      this.zkCoord = (ZKClusterCoordinator) coord;
    } else {
      this.zkCoord = null;
    }

    // Query Profile Store
    QueryProfileStoreContext pStoreContext = context.getProfileStoreContext();
    this.completedProfileStore = (LocalPersistentStore<QueryProfile>) pStoreContext.getCompletedProfileStore();
    this.pStoreConfig = pStoreContext.getProfileStoreConfig();
    this.basePath = completedProfileStore.getBasePath();

    this.indexingRate = drillConfig.getInt(ExecConstants.PROFILES_STORE_INDEX_MAX);
    this.profiles = new ProfileSet(indexingRate);
    this.indexWatch = Stopwatch.createUnstarted();
    this.sysFileSuffixFilter = new DrillSysFilePathFilter();
    //TODO / FIXME (read from config?)
    String indexPathPattern =
        drillConfig.getString(ExecConstants.PROFILES_STORE_INDEX_FORMAT);
//        "yyyy/MM/dd"; //TODO ExecConstants
    this.indexedPathFormat = new SimpleDateFormat(indexPathPattern);
    logger.info("Index Format : {}", indexedPathFormat.toPattern());
  }


  //Synchronized Profile
  public void indexProfiles() {
    /**
     * 1. Get ZK Lock
     * 2. Build list of files
     * 3. Retain latest (archiving them first ensures they are accessed fastest as well)
     * 4. Index them
     */

    if (useZkCoordinatedManagement) {
      // TODO Acquire lock IFF required
      DistributedSemaphore indexerMutex = new ZkDistributedSemaphore(zkCoord.getCurator(), lockPathString, 1);
      try (DistributedLease lease = indexerMutex.acquire(0, TimeUnit.SECONDS)) {
        if (lease != null) {
          listAndIndex();
        } else {
          logger.info("Couldn't get a lease acquisition");
        }
      } catch (Exception e) {
        //DoNothing since lease acquisition failed
        logger.info("Exception during lease-acquisition:: {}", e);
      }
    } else {
      try {
        listAndIndex();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }


  //Lists and Indexes the latest profiles
  private void listAndIndex() throws IOException {
    int currentProfileCount = listForArchiving(), indexedCount = 0;
    //TODO
    logger.info("Found {} profiles that need to be indexed. Will attempt to index {} profiles", currentProfileCount,
        (currentProfileCount > this.indexingRate) ? this.indexingRate : currentProfileCount);

    Map<String, Path> mruIndexPath = new HashMap<>();

    if (currentProfileCount > 0) {
      while (!this.profiles.isEmpty()) {
        String profileToIndex = profiles.removeYoungest() + DRILL_SYS_FILE_SUFFIX;
        Path srcPath = new Path(basePath, profileToIndex);
        long profileStartTime = getProfileStart(srcPath);
        if (profileStartTime < 0) {
          logger.info("Will skip indexing {}", srcPath);
          continue;
        }
        String indexPath = indexedPathFormat.format(new Date(profileStartTime));
        logger.info("{}  <--<  {} [{}]", indexPath, srcPath, profileStartTime);
        //Check if exists
        Path indexDestPath = null;
        if (!mruIndexPath.containsKey(indexPath)) {
          indexDestPath = new Path(basePath, indexPath);
          logger.info("Check existence of {} dir", indexDestPath);
          if (!fs.isDirectory(indexDestPath)) {
            // Build dir
            if (fs.mkdirs(indexDestPath)) {
              mruIndexPath.put(indexPath, indexDestPath);
            } else {
              //Creation failed. Did someone else create? Check before throwing error
              if (fs.isDirectory(indexDestPath)) {
                mruIndexPath.put(indexPath, indexDestPath);
              } else {
                //TODO Throw Error (N times before exiting?)
              }
            }
          } else {
            mruIndexPath.put(indexPath, indexDestPath);
          }
        } else {
          indexDestPath = mruIndexPath.get(indexPath);
        }

        //Attempt Move
        boolean renameStatus = false;
        if (indexDestPath != null) {
          Path destPath = new Path(indexDestPath, profileToIndex);
          logger.info("MOVE {} ---> {}", srcPath.toUri(), destPath.toUri());
          renameStatus = DrillFileSystemUtil.rename(fs, srcPath, destPath);
          if (renameStatus) {
            indexedCount++;
          }
        }
        if (indexDestPath == null || !renameStatus) {
          // Stop attempting any more archiving since other StoreProviders might be archiving
          logger.error("Move failed for {} [{} | {}]", srcPath, indexDestPath == null, renameStatus);
          continue;
        }
      }
    }

    logger.info("Successfully indexed {} profiles during startup", indexedCount);
  }

  //Extracts the profile's start time
  private long getProfileStart(Path srcPath) {
    try (InputStream is = fs.open(srcPath)) {
      QueryProfile profile = pStoreConfig.getSerializer().deserialize(IOUtils.toByteArray(is));
      return profile.getStart();
    } catch (IOException e) {
      logger.info("Unable to deserialize {}\n---{}====", srcPath, e.getMessage()); //Illegal character ((CTRL-CHAR, code 0)): only regular white space (\r, \n, \t) is allowed between tokens       at [Source: [B@f76ca5b; line: 1, column: 65538]
      logger.info("deserialization RCA==> \n {}", ExceptionUtils.getRootCause(e));
      //DontNeedThis: throw new RuntimeException("Unable TO deSerialize \"" + srcPath, ExceptionUtils.getRootCause(e));
    }
    return Long.MIN_VALUE;
  }

  // List all profiles in store's root and identify potential candidates for archiving
  private int listForArchiving() throws IOException {
    List<FileStatus> fileStatuses = DrillFileSystemUtil.listFiles(fs, basePath, false, //Not performing recursive search of profiles
        sysFileSuffixFilter
        );

    //Populating cache with profiles
    int numProfilesInStore = 0;

    for (FileStatus stat : fileStatuses) {
      String profileName = stat.getPath().getName();
      //Strip extension and store only query ID
      profiles.add(profileName.substring(0, /*TODO make it a constant*/profileName.length() - DRILL_SYS_FILE_EXT_SIZE), false);
      numProfilesInStore++;
    }

    return numProfilesInStore;
  }

  //Infers File System of Local Store
  private DrillFileSystem inferFileSystem(DrillConfig drillConfig) throws IOException {
    boolean hasZkBlobRoot = drillConfig.hasPath(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT);
    final Path blobRoot = hasZkBlobRoot ?
        new org.apache.hadoop.fs.Path(drillConfig.getString(ZookeeperPersistentStoreProvider.DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT)) :
          LocalPersistentStore.getLogDir();

    return LocalPersistentStore.getFileSystem(drillConfig, blobRoot);
  }

  //Check if distributed FS
  private boolean isFileSysDistributed(DrillFileSystem fileSystem) {
    final List<String> supportedFS = drillConfig.getStringList(ExecConstants.PROFILES_STORE_INDEX_SUPPORTED_FS);
    logger.info("{} ? --> {} ", fs.getScheme(), supportedFS.toString());
    if (supportedFS.contains(fileSystem.getScheme())) {
      return true;
    }
    return false;
  }
}
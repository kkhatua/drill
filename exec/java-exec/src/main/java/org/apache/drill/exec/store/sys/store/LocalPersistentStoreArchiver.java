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
package org.apache.drill.exec.store.sys.store;

import static org.apache.drill.exec.ExecConstants.DRILL_SYS_FILE_SUFFIX;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archive profiles
 * @param <V>
 */
public class LocalPersistentStoreArchiver<V> {
  private static final Logger logger = LoggerFactory.getLogger(LocalPersistentStoreArchiver.class);

  private static final String ARCHIVE_LOCATION = "archived";

  private final DrillFileSystem fs;
  private Path basePath;
  private Path archivePath;
  private ProfileSet pendingArchivalSet;
  private int archivalThreshold;
  private int archivalRate;
  private Stopwatch archiveWatch;
  //TODO: Make this one time
  private LocalPersistentStore<V> store;
  private boolean organizeIntoPigeonHoles;
  private SimpleDateFormat pigeonHoleFormat;
  private Map<String,Path> pigeonHoleMap;

  public LocalPersistentStoreArchiver(DrillFileSystem fs, Path base, DrillConfig drillConfig, LocalPersistentStore<V> lpStore) throws IOException {
    this.store = lpStore;
    this.fs = fs;
    this.basePath = base;
    this.archivalThreshold = drillConfig.getInt(ExecConstants.PROFILES_STORE_CAPACITY);
    this.archivalRate = drillConfig.getInt(ExecConstants.PROFILES_STORE_ARCHIVE_RATE);
    this.pendingArchivalSet = new ProfileSet(archivalRate);
    this.archivePath = new Path(basePath, ARCHIVE_LOCATION);
    this.archiveWatch = Stopwatch.createUnstarted();
    this.organizeIntoPigeonHoles = drillConfig.getBoolean(ExecConstants.PROFILES_STORE_ARCHIVE_ORGANIZE_ENABLED);
    this.pigeonHoleFormat = null;
    if (organizeIntoPigeonHoles) {
      this.pigeonHoleFormat = new SimpleDateFormat(drillConfig.getString(ExecConstants.PROFILES_STORE_ARCHIVE_ORGANIZE_FORMAT));
      this.pigeonHoleMap = new HashMap<>();
    }

    try {
      if (!fs.exists(archivePath)) {
        fs.mkdirs(archivePath);
      }
    } catch (IOException e) {
      logger.error("Disabling profile archiving due to failure in creating profile archive {} : {}", archivePath, e);
      throw e;
    }
  }

  /**
   *Initiates archiving
   * @param profilesInStoreCount
   */
  void archiveProfiles(int profilesInStoreCount) {
    if (profilesInStoreCount > archivalThreshold) {
      //We'll attempt to reduce to 90% of threshold, but in batches of archivalRate
      int excessCount = profilesInStoreCount - (int) Math.round(0.9*archivalThreshold);
      int numToArchive = Math.min(excessCount, archivalRate);
      logger.info("Found {} excess profiles. For now, will attempt archiving {} profiles to {}", excessCount,
          numToArchive, archivePath);
      int archivedCount = 0;
      try {
        if (fs.isDirectory(archivePath)) {
          //FIXME PigeonHoleFormat status?
          logger.info("PigeonHoleFormat : {}", pigeonHoleFormat);
          if (organizeIntoPigeonHoles) {
            this.pigeonHoleMap.clear();
          }
          archiveWatch.reset().start(); //Clocking
          while (!pendingArchivalSet.isEmpty()) {
            String queryIdAsString = pendingArchivalSet.removeOldest();
            Path archiveDestPath = organizeIntoPigeonHoles ? extractPigeonHolePath(queryIdAsString) : archivePath;
            String toArchive = queryIdAsString + DRILL_SYS_FILE_SUFFIX;
            boolean renameStatus = DrillFileSystemUtil.rename(fs,
                new Path(basePath, toArchive),
                new Path(archiveDestPath, toArchive));
            if (!renameStatus) {
              //Stop attempting any more archiving since other StoreProviders might be archiving
              logger.error("Move failed for {} from {} to {}", toArchive, basePath.toString(), archiveDestPath.toString());
              logger.warn("Skip archiving under the assumption that another Drillbit is archiving");
              break;
            }
            archivedCount++;
          }
          logger.info("Archived {} profiles to {} in {} ms", archivedCount, archivePath, archiveWatch.stop().elapsed(TimeUnit.MILLISECONDS));
        } else {
          logger.error("Unable to archive {} profiles to {}", pendingArchivalSet.size(), archivePath.toString());
        }
      } catch (IOException e) {
        logger.error("Unable to archive profiles to {} ({} successful) due to {}", archivePath.toString(), archivedCount, e.getLocalizedMessage());
      }
    }
    //Clean up
    clearPending();
  }

  //Extracts the time and applies for time format
  @SuppressWarnings("unused")
  private Path extractPigeonHolePath(String queryIdAsString)  {
    V storeObj = store.get(queryIdAsString);
    if (storeObj instanceof QueryProfile) { //We do extraction for Query Profiles ONLY
      QueryProfile profile = (QueryProfile) storeObj;
      String pigeonHole = pigeonHoleFormat.format(new Date(profile.getStart()));
      Path archiveDestPath = new Path(archivePath, pigeonHole);
      if (!pigeonHoleMap.containsKey(pigeonHole)) { //Saves calls to the FS
        //Check if directory was created
        try {
          if (!fs.isDirectory(archiveDestPath)) {
            //Create
            boolean mkdirPigeonHoleStatus = fs.mkdirs(archiveDestPath);
            logger.info("Tried to create {} with status={}", archiveDestPath, mkdirPigeonHoleStatus);
            if (!mkdirPigeonHoleStatus) {
              //TODO: Fix message... but dont fail
              logger.warn("Failed to create pigeonhole. Writing at default");
              return archivePath;
            }
          }
          pigeonHoleMap.put(pigeonHole, archiveDestPath);
          return archiveDestPath;
        } catch (IOException e) {
          logger.warn("Failed to create pigeonhole: {}", e.getMessage());
        }
      } else {
        return pigeonHoleMap.get(pigeonHole);
      }
    }
    //Empty
    return archivePath;
  }

  /**
   * Clears the remaining pending profiles
   */
  public void clearPending() {
    this.pendingArchivalSet.clear();
  }

  /**
   * Add a profile for archiving
   * @param profileName
   * @return youngest profile that will not be archived
   */
  public String addProfile(String profileName) {
    return this.pendingArchivalSet.add(profileName, true);
  }

  Path getArchivePath() {
    return archivePath;
  }

  SimpleDateFormat getPigeonHoleFormat() {
    return pigeonHoleFormat;
  }

}

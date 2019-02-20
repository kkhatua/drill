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
import java.net.InetAddress;
import java.net.UnknownHostException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashMap;
import java.util.List;
//import java.util.Map;
import java.util.concurrent.TimeUnit;

//import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DistributedSemaphore.DistributedLease;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZkDistributedSemaphore;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.ProfileManagerContext;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.store.DrillSysFilePathFilter;
import org.apache.drill.exec.store.sys.store.ProfileSet;
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
public class ProfileManager extends Thread implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ProfileManager.class);
  //TODO
  private static final String lockPathString = "/profileManager";
  private static final String PROFILE_MANAGER_THREAD = "ProfileManager";
  private static final int DRILL_SYS_FILE_EXT_SIZE = DRILL_SYS_FILE_SUFFIX.length();

  private final ZKClusterCoordinator zkCoord;
  private final DrillFileSystem fs;
  private final Path basePath;
  private final ProfileSet profiles;
  private final Path archivePath;
  private final ProfileSet pendingArchival;
  private final long interval;
  private final int archivalThreshold;
  private final int archivalRate;
  private final PathFilter sysFileSuffixFilter;
  private Stopwatch archiveWatch;
  //TODO: Make this one time
//  private boolean organizeIntoPigeonHoles;
//  private SimpleDateFormat pigeonHoleFormat;
//  private Map<String,Path> pigeonHoleMap;



  public ProfileManager(ProfileManagerContext profileManagerContext) throws StoreException, IOException {
    this.zkCoord = profileManagerContext.getZkCoord();
    this.fs = profileManagerContext.getFileSystem();
    this.basePath = profileManagerContext.getCompletedProfileStore().getBasePath();
    this.archivalThreshold = profileManagerContext.getArchivalThreshold();
    this.archivalRate = profileManagerContext.getArchivalRate();
    this.profiles = new ProfileSet(archivalThreshold);
    this.pendingArchival = new ProfileSet(archivalRate);
    this.archivePath = profileManagerContext.getArchivalPath();
    this.interval = profileManagerContext.getArchivalInterval();
    this.archiveWatch = Stopwatch.createUnstarted();
    this.sysFileSuffixFilter = new DrillSysFilePathFilter();
    //this.organizeIntoPigeonHoles = profileManagerContext.isOrganizedIntoPigeonHoles();
//    this.pigeonHoleFormat = null;
//    if (organizeIntoPigeonHoles) {
//      this.pigeonHoleFormat = profileManagerContext.getPigeonHoleFormat();
//      this.pigeonHoleMap = new HashMap<>();
//    }

    try {
      if (!fs.exists(archivePath)) {
        fs.mkdirs(archivePath);
      }
    } catch (IOException e) {
      logger.error("Disabling profile archiving due to failure in creating profile archive {} : {}", archivePath, e);
      throw e;
    }
    logger.info("Initialized QueryProfilesManager with archive at :: {}", archivePath);
  }

  /**
   * Initiates archiving
   * @param profilesInStoreCount
   */
  void archiveProfiles(int profilesInStoreCount) {

    //We'll attempt to reduce to 90% of threshold, but in batches of archivalRate
    int excessCount = profilesInStoreCount - (int) Math.round(0.9*archivalThreshold);
    int numToArchive = Math.min(excessCount, archivalRate);
    logger.info("Found {} excess profiles. For now, will attempt archiving {} profiles to {}", excessCount,
        numToArchive, archivePath);
    int archivedCount = 0;
    try {
      if (fs.isDirectory(archivePath)) {
        //FIXME PigeonHoleFormat status?
//        logger.info("PigeonHoleFormat : {}", pigeonHoleFormat);
//        if (organizeIntoPigeonHoles) {
//          this.pigeonHoleMap.clear();
//        }
        archiveWatch.reset().start(); //Clocking
        while (!pendingArchival.isEmpty()) {
          String queryIdAsString = pendingArchival.removeOldest();
          Path archiveDestPath = /*organizeIntoPigeonHoles ? extractPigeonHolePath(queryIdAsString) : */archivePath;
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
        logger.error("Unable to archive {} profiles to {}", pendingArchival.size(), archivePath.toString());
      }
    } catch (IOException e) {
      logger.error("Unable to archive profiles to {} ({} successful) due to {}", archivePath.toString(), archivedCount, e.getLocalizedMessage());
    }
  }

  /**
   * Clears the remaining pending profiles
   */
  public void clearPending() {
    this.pendingArchival.clear();
  }

  /**
   * Add a profile for archiving
   * @param profileName
   * @return youngest profile that will not be archived
   */
  public String addProfileForArchiving(String profileName) {
    return this.pendingArchival.add(profileName, true);
  }

  @SuppressWarnings("unused")
  @Override
  public void run() {
    setName(PROFILE_MANAGER_THREAD);
    int temp=0;

    DistributedSemaphore managerMutex = new ZkDistributedSemaphore(zkCoord.getCurator(), lockPathString, 1);
    DistributedLease lease = null;

    //[dBug]
    String myName = "unknown";
    try {
      myName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }

    while (/*temp++ < 10*/true) {
      //logger.info("{} is simulating archiving by acquiring lock ", myName);

      /*
       * 1. Get lock
       * 2. Inspect files
       * 3. Archive some files
       * 4. Update entry?
       * 5. Close/release lock
       * Ref: https://dzone.com/articles/distributed-lock-using
       */

      //Pause:: logger.info("Pausing for {}", interval);
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(interval));
      } catch (final InterruptedException e) {
        // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
        // interruption and respond to it if it wants to.
        Thread.currentThread().interrupt();

        // exit status thread on interrupt.
        break;
      }

      try {
        lease = managerMutex.acquire(0, TimeUnit.SECONDS);
        if (lease == null) {
//          logger.info("Couldn't acquire lease");
          continue;
//        } else {
//          logger.info("Acquired Lease {}", lease);
        }

        int currentProfileCount = listForArchiving();

        logger.info("Found {} profiles and {} can be archived (threshold={})", currentProfileCount, this.pendingArchival.size(), archivalThreshold);
        if (currentProfileCount > archivalThreshold) {
          archiveProfiles(currentProfileCount);
        }

        //Clean up the pendingArchivalSet
        clearPending();
        //Done archving
      } catch (Exception e) {
        // TODO: handle exception
      } finally {
        if (lease != null) {
          try {
            lease.close();
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
    }

    logger.info("=====End-Of-Day-s-Play=====");
  }

  //Shuts down the thread
  @Override
  public void close() {
    this.interrupt();
  }

  //List all profiles in store's root and identify potential candidates for archiving
  private int listForArchiving() throws IOException {
//    logger.info("Listing on {}\t{}", fs.toString(), basePath);

    List<FileStatus> fileStatuses = DrillFileSystemUtil.listFiles(fs, basePath, false, //Not performing recursive search of profiles
        sysFileSuffixFilter /*TODO: Use MostRecentProfile */
        );

    //Populating cache with profiles
    int numProfilesInStore = 0;

    for (FileStatus stat : fileStatuses) {
      String profileName = stat.getPath().getName();
      //Strip extension and store only query ID
      String oldestProfile = profiles.add(profileName.substring(0, /*TODO make it a constant*/profileName.length() - DRILL_SYS_FILE_EXT_SIZE), false);
      if (oldestProfile != null) {
        this.pendingArchival.add(oldestProfile, true);
      }
      numProfilesInStore++;
    }

    return numProfilesInStore;
  }

  /*
  //Extracts the time and applies for time format
  @SuppressWarnings("unused")
  private Path extractPigeonHolePath(String queryIdAsString)  {
    QueryProfile profile = store.get(queryIdAsString);
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
    //Empty
    return archivePath;
  }
*/
}

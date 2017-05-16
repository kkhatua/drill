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
package org.apache.drill.exec.store.sys;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.foreman.QueryManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * DRILL-5068: Add a new system table for completed profiles
 */
public class ProfileIterator implements Iterator<Object> {
  private static final String EXECUTION_TIME = "execution";
  private static final String ENQUEUED_TIME = "enqueue";
  private static final String PLANNING_TIME = "planning";
  private static final String QUERY_DURATION = "duration";

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileIterator.class);

  private final Iterator<ProfileInfo> itr;
  private final DrillbitContext dbitContext;

  public ProfileIterator(FragmentContext context) {
    dbitContext = context.getDrillbitContext();
    itr = iterateProfileInfo(context);
  }

  private Iterator<ProfileInfo> iterateProfileInfo(FragmentContext context) {
    try {
      PersistentStore<UserBitShared.QueryProfile> profiles = context
          .getDrillbitContext()
          .getProfileStoreContext().getCompletedProfileStore();

      return transform(profiles.getAll());

    } catch (Exception e) {
      logger.error(String.format("Unable to get persistence store: %s, ", context.getDrillbitContext().getProfileStoreContext().getCompletedProfileStore()), e);
      return Iterators.singletonIterator(ProfileInfo.getDefault());
    }
  }

  /**
   * Iterating persistentStore as a iterator of {@link org.apache.drill.exec.store.sys.ProfileIterator.ProfileInfo}.
   */
  private Iterator<ProfileInfo> transform(Iterator<Map.Entry<String, UserBitShared.QueryProfile>> all) {
    return Iterators.transform(all, new Function<Map.Entry<String, UserBitShared.QueryProfile>, ProfileInfo>() {
      @Nullable
      @Override
      public ProfileInfo apply(@Nullable Map.Entry<String, UserBitShared.QueryProfile> input) {
        if (input == null || input.getValue() == null) {
          return ProfileInfo.getDefault();
        }

        final String queryID = input.getKey();
        final QueryProfile queryProfile = input.getValue();

        return new ProfileInfo(queryID,
            input.getValue().getForeman().getAddress(),
            new Timestamp(queryProfile.getStart()),
            estimateTime(queryProfile, PLANNING_TIME),
            estimateTime(queryProfile, ENQUEUED_TIME),
            estimateTime(queryProfile, EXECUTION_TIME),
            estimateTime(queryProfile, QUERY_DURATION),
            queryProfile.getUser(),
            queryProfile.getQuery(),
            queryProfile.getState().name(),
            getProfileJSON(queryProfile)
            );
      }

      /**
       * Provide the Profile's JSON as a String
       * @param queryProfile Query profile as a query-id defined entry
       * @return
       */
      private String getProfileJSON(QueryProfile queryProfile) {
        String profileJson = "{}";
        try {
          profileJson = new String(dbitContext.getProfileStoreContext().getProfileStoreConfig().getSerializer().serialize(queryProfile));
        } catch (IOException e) {
          e.printStackTrace();
        }
        return profileJson;
      }

      /**
       * Estimate Time
       * @param queryProfile
       * @param string
       * @return
       */
      private long estimateTime(QueryProfile queryProfile,
          String string) {
        switch (string) {
        case PLANNING_TIME:
          return (queryProfile.getPlanEnd() > 0) ? queryProfile.getPlanEnd() - queryProfile.getStart() : 0L;
        case ENQUEUED_TIME:
          return (queryProfile.getQueueWaitEnd() > 0) ? queryProfile.getQueueWaitEnd() - queryProfile.getPlanEnd() : 0L;
        case EXECUTION_TIME:
          long executionEnd = (queryProfile.getQueueWaitEnd() > 0 ? queryProfile.getQueueWaitEnd() :
            queryProfile.getPlanEnd() > 0 ? queryProfile.getPlanEnd() : queryProfile.getStart());
          return queryProfile.getEnd() - executionEnd;
        case QUERY_DURATION:
          return queryProfile.getEnd() - queryProfile.getStart();
        default:
          //Do Nothing
        break;
        }
        //Default
        return 0;
      }

    });
  }

  @Override
  public boolean hasNext() {
    return itr.hasNext();
  }

  @Override
  public Object next() {
    return itr.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public static class ProfileInfo {
    private static final ProfileInfo DEFAULT = new ProfileInfo();

    public final String query_id;
    public final String foreman;
    public final Timestamp time;
    public final long planningTime;
    public final long enqueueTime;
    public final long executionTime;
    public final long latency;
    public final String user;
    public final String query;
    public final String state;
    public final String json;

    public ProfileInfo(String query_id, String foreman, Timestamp time, long planningTime, long enqueueTime, long executionTime, long latency, String user, String query, String state, String json) {
      this.query_id = query_id;
      this.foreman = foreman;
      this.time = time;
      this.planningTime = planningTime;
      this.enqueueTime = enqueueTime;
      this.executionTime = executionTime;
      this.latency = latency;
      this.user = user;
      this.query = query;
      this.state = state;
      this.json = json;
    }

    private ProfileInfo() {
      this("UNKNOWN", "UNKNOWN", new Timestamp(0L), 0L, 0L, 0L, 0L, "UNKNOWN", "UNKNOWN", "UNKNOWN", "{}");
    }

    /**
     * If unable to get ProfileInfo, use this default instance instead.
     * @return the default instance
     */
    public static final ProfileInfo getDefault() {
      return DEFAULT;
    }
  }
}
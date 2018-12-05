/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.runner.production;

import com.google.common.base.Throwables;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.SourceOffsetJson;
import org.apache.curator.framework.CuratorFramework;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Persistent storage for pipeline offsets.
 */
public interface OffsetStorage {
  void resetOffsets(RuntimeInfo runtimeInfo, String pipelineName, String rev);

  void saveOffsets(RuntimeInfo runtimeInfo, String pipelineName, String rev, Map<String, String> offset);

  Map<String, String> getAndSaveIfEmpty(RuntimeInfo runtimeInfo, String pipelineName, String rev);

  /**
   * File-based storage.
   */
  class FileStorage implements OffsetStorage {
    public static final String STORAGE_TYPE = "file";

    @Override
    public void resetOffsets(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
      OffsetFileUtil.resetOffsets(runtimeInfo, pipelineName, rev);
    }

    @Override
    public void saveOffsets(RuntimeInfo runtimeInfo, String pipelineName, String rev, Map<String, String> offset) {
      OffsetFileUtil.saveOffsets(runtimeInfo, pipelineName, rev, offset);
    }

    @Override
    public Map<String, String> getAndSaveIfEmpty(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
      return OffsetFileUtil.saveIfEmpty(runtimeInfo, pipelineName, rev);
    }
  }

  /**
   * Zookeeper-based storage.
   */
  class ZookeeperStorage implements OffsetStorage {
    private static final Map<String, String> DEFAULT_OFFSET = Collections.emptyMap();
    public static final String STORAGE_TYPE = "zookeeper";
    public static final String ADDRESS_PROPERTY = "offsets.storage.zookeeper.address";
    public static final String PREFIX_PROPERTY = "offsets.storage.zookeeper.prefix";
    public static final String TIMEOUT_PROPERTY = "offsets.storage.zookeeper.connect_timeout";
    public static final String RECONNECT_PAUSE_PROPERTY = "offsets.storage.zookeeper.reconnect_pause";
    public static final int TIMEOUT_DEFAULT = 5000;
    public static final int RECONNECT_PAUSE_DEFAULT = 500;

    private final CuratorFramework client;
    private final String prefix;
    private boolean nodeExits = false;

    public ZookeeperStorage(CuratorFramework client, String prefix) {
      this.client = client;
      this.prefix = prefix;
    }

    @Override
    public void resetOffsets(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
      saveOffsets(runtimeInfo, pipelineName, rev, DEFAULT_OFFSET);
    }

    @Override
    public void saveOffsets(RuntimeInfo runtimeInfo, String pipelineName, String rev, Map<String, String> offset) {
      try {
        createNodeIfRequired(pipelineName, rev);
        client.setData().forPath(path(pipelineName, rev), toBytes(offset));
      } catch (Exception e) {
        // TODO reviewer, what exception class should be used here?
        throw Throwables.propagate(e);
      }
    }

    @Override
    public Map<String, String> getAndSaveIfEmpty(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
      try {
        createNodeIfRequired(pipelineName, rev);
        byte[] bytes = client.getData().forPath(path(pipelineName, rev));
        return fromBytes(bytes).getOffsets();
      } catch (Exception e) {
        // TODO reviewer, what exception class should be used here?
        throw Throwables.propagate(e);
      }
    }

    @NotNull
    private SourceOffset fromBytes(byte[] bytes) throws IOException {
      SourceOffsetJson sourceOffsetJson = ObjectMapperFactory.get().readValue(bytes, SourceOffsetJson.class);
      SourceOffset sourceOffset = BeanHelper.unwrapSourceOffset(sourceOffsetJson);
      SourceOffsetUpgrader.upgrade(sourceOffset);
      return sourceOffset;
    }

    private byte[] toBytes(Map<String, String> offsets) throws IOException {
      SourceOffset sourceOffset = new SourceOffset(SourceOffset.CURRENT_VERSION, offsets);
      return ObjectMapperFactory.get().writeValueAsBytes(BeanHelper.wrapSourceOffset(sourceOffset));
    }

    private synchronized void createNodeIfRequired(String pipelineName, String rev) throws Exception {
      if (!nodeExits) {
        String path = path(pipelineName, rev);
        if (client.checkExists().forPath(path) == null) {
          // create new node and save default offsets value
          client.create().creatingParentsIfNeeded().forPath(path, toBytes(DEFAULT_OFFSET));
        }
        nodeExits = true;
      }
    }

    private String path(String pipelineName, String rev) {
      String sanitizedName = pipelineName.replaceAll("/", "_");
      String sanitizedPrefix = prefix;
      if (sanitizedPrefix.startsWith("/")) {
        sanitizedPrefix = sanitizedPrefix.substring(1);
      }
      if (sanitizedPrefix.endsWith("/")) {
        sanitizedPrefix = sanitizedPrefix.substring(0, sanitizedPrefix.length() - 1);
      }

      return String.format("/%s/%s/%s", sanitizedPrefix, sanitizedName, rev);
    }
  }
}

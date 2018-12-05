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

import com.streamsets.datacollector.util.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for {@link OffsetStorage}.
 */
public interface OffsetStorageFactory {
  OffsetStorageFactory FILE = new FileOffsetStorageFactory();

  OffsetStorage create(Configuration configuration);

  /**
   * Creates {@link com.streamsets.datacollector.runner.production.OffsetStorage.FileStorage}.
   */
  class FileOffsetStorageFactory implements OffsetStorageFactory {
    @Override
    public OffsetStorage create(Configuration configuration) {
      return new OffsetStorage.FileStorage();
    }
  }

  /**
   * Singleton factory instance. Caches zookeeper client and shares it between all zookeeper-based offset storages.
   */
  class OffsetStorageFactoryImpl implements OffsetStorageFactory {
    public static final OffsetStorageFactoryImpl INSTANCE = new OffsetStorageFactoryImpl();

    // cache for zookeeper clients
    private final ConcurrentHashMap<String, CuratorFramework> zookeeperClients = new ConcurrentHashMap<>();

    private OffsetStorageFactoryImpl() {
    }

    @Override
    public OffsetStorage create(Configuration config) {
      String storageType = config.get(
          ProductionSourceOffsetTracker.STORAGE_TYPE,
          OffsetStorage.FileStorage.STORAGE_TYPE
      );

      switch (storageType) {
        case OffsetStorage.FileStorage.STORAGE_TYPE:
          return new OffsetStorage.FileStorage();
        case OffsetStorage.ZookeeperStorage.STORAGE_TYPE:
          return createZookeeperStorage(config);
        default:
          // TODO reviewer, what exception class should be used here?
          throw new IllegalStateException(String.format("Unknown storage type '%s'", storageType)); // TODO
      }
    }

    private OffsetStorage.ZookeeperStorage createZookeeperStorage(Configuration config) {
      String zookeeperAddress = config.get(OffsetStorage.ZookeeperStorage.ADDRESS_PROPERTY, "unknown");
      // TODO reviewer, what is preferred way to handle missing config options? What exception class should be thrown?
      if ("unknown".equals(zookeeperAddress)) {
        throw new IllegalStateException(
            String.format(
                "Zookeeper address not configured, set it via '%s' sdc configuration property",
                OffsetStorage.ZookeeperStorage.ADDRESS_PROPERTY
            )
        );
      }

      int timeout = config.get(OffsetStorage.ZookeeperStorage.TIMEOUT_PROPERTY, OffsetStorage.ZookeeperStorage.TIMEOUT_DEFAULT);
      int pause = config.get(OffsetStorage.ZookeeperStorage.RECONNECT_PAUSE_PROPERTY, OffsetStorage.ZookeeperStorage.RECONNECT_PAUSE_DEFAULT);

      CuratorFramework client = zookeeperClients.computeIfAbsent(zookeeperAddress, (address) -> {
        CuratorFramework zoo = CuratorFrameworkFactory.newClient(
            address,
            new RetryUntilElapsed(timeout, pause)
        );
        zoo.start();
        return zoo;
      });

      String prefix = config.get(OffsetStorage.ZookeeperStorage.PREFIX_PROPERTY, "");
      return new OffsetStorage.ZookeeperStorage(client, prefix);
    }
  }
}

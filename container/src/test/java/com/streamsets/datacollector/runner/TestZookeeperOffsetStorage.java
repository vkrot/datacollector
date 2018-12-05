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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.production.OffsetStorage;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Map;

public class TestZookeeperOffsetStorage {

  @Rule
  public TemporaryFolder snapDir = new TemporaryFolder();
  public TemporaryFolder logDir = new TemporaryFolder();
  private NIOServerCnxnFactory factory;
  private CuratorFramework client;
  private int port;

  @Before
  public void startZookeeper() throws Exception {
    snapDir.create();
    logDir.create();

    port = getFreePort();
    ZooKeeperServer server = new ZooKeeperServer(snapDir.newFolder(), logDir.newFolder(), 1000);
    factory = new NIOServerCnxnFactory();
    factory.configure(new InetSocketAddress("localhost", port), 16);
    factory.startup(server);
    client = CuratorFrameworkFactory.newClient("localhost:" + port, new RetryUntilElapsed(2000, 100));
    client.start();
  }

  public void stopZookeeper() throws Exception {
    client.close();
    factory.shutdown();
  }

  @Test
  public void testSaveAndGetOffsets() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);

    OffsetStorage.ZookeeperStorage storage = new OffsetStorage.ZookeeperStorage(client, "sdc");
    Map<String, String> offsets = ImmutableMap.of("a", "b", "c", "d");
    storage.saveOffsets(runtimeInfo, "a", "0", offsets);
    Assert.assertEquals(
        storage.getAndSaveIfEmpty(runtimeInfo, "a", "0"),
        offsets
    );
  }

  @Test
  public void testGetEmptyOffsets() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);

    OffsetStorage.ZookeeperStorage storage = new OffsetStorage.ZookeeperStorage(client, "sdc");
    Assert.assertEquals(
      storage.getAndSaveIfEmpty(runtimeInfo, "a", "1"),
      Collections.emptyMap()
    );
  }

  @Test
  public void testResetOffsets() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);

    OffsetStorage.ZookeeperStorage storage = new OffsetStorage.ZookeeperStorage(client, "sdc");
    Map<String, String> offsets = ImmutableMap.of("a", "b", "c", "d");
    storage.saveOffsets(runtimeInfo, "a", "0", offsets);
    Assert.assertEquals(
        storage.getAndSaveIfEmpty(runtimeInfo, "a", "0"),
        offsets
    );
    storage.resetOffsets(runtimeInfo, "a", "0");

    Assert.assertEquals(
        storage.getAndSaveIfEmpty(runtimeInfo, "a", "0"),
        Collections.emptyMap()
    );
  }

  public static int getFreePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }
}

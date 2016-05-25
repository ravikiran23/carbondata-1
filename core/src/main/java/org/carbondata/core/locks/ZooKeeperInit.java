package org.carbondata.core.locks;

import java.io.IOException;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * This is a singleton class for initialization of zookeeper client.
 */
public class ZooKeeperInit {

  /**
   * zk is the zookeeper client instance
   */
  private ZooKeeper zk;

  private static ZooKeeperInit zooKeeperInit;

  /**
   * zooKeeperLocation is the location in the zoo keeper file system where the locks will be
   * maintained.
   */
  public static final String zooKeeperLocation = CarbonCommonConstants.ZOOKEEPER_LOCATION;

  private ZooKeeperInit(String zooKeeperUrl) {

    int sessionTimeOut = 100000;
    try {
      zk = new ZooKeeper(zooKeeperUrl, sessionTimeOut, new Watcher() {

        @Override public void process(WatchedEvent event) {
          if (event.getState().equals(Event.KeeperState.SyncConnected)) {
            // if exists returns null then path doesn't exist. so creating.
            try {
              if (null == zk.exists(zooKeeperLocation, true)) {
                // creating a znode in which all the znodes (lock files )are maintained.
                zk.create(zooKeeperLocation, new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
              }
            } catch (KeeperException | InterruptedException e) {
              LOGGER.error(e.getMessage());
            }
            LOGGER.info("zoo keeper client connected.");
          }

        }
      });

    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }

  }

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ZooKeeperInit.class.getName());

  public static ZooKeeperInit getInstance(String zooKeeperUrl) {

    if (null == zooKeeperInit) {
      synchronized (ZooKeeperInit.class) {
        if (null == zooKeeperInit) {
          zooKeeperInit = new ZooKeeperInit(zooKeeperUrl);
        }
      }
    }
    return zooKeeperInit;

  }

  public static ZooKeeperInit getInstance() {
    return zooKeeperInit;
  }

  public ZooKeeper getZookeeper() {
    return zk;
  }

}

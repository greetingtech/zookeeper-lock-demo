package com.greetingtech.zk.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author greetingtech
 * @date 2019-11-16
 */
public class ZkLockFactory {

    private final ZooKeeper zk;

    private final String lockDir;

    public ZkLockFactory(ZooKeeper zk, String lockDir) {
        this.zk = zk;
        this.lockDir = lockDir;
        insureDirExists(lockDir);
    }

    public ZkLock createLock(String lockKey) {
        String realLockDir = lockDir + lockKey;
        insureDirExists(realLockDir);
        return new ZkLock(zk, realLockDir);
    }

    private void insureDirExists(String dir) {
        try {
            zk.create(dir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (InterruptedException e) {
            throw new ZkLockException("interrupted when create dir");
        } catch (KeeperException e) {
            if (!KeeperException.Code.NODEEXISTS.equals(e.code())) {
                throw new ZkLockException(e);
            }
        }
    }

}

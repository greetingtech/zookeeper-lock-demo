package com.greetingtech.zk.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

/**
 * @author greetingtech
 * @date 2019-11-16
 */
public class ZkLock {

    private final ZooKeeper zk;

    private final String lockDir;

    private final String followerDir;

    private String lockNode;

    private Thread lockOwner;

    public ZkLock(ZooKeeper zk, String lockDir) {
        this.zk = zk;
        this.lockDir = lockDir;
        this.followerDir = lockDir + "/followers";
    }

    public void lock() {
        try {
            doLock();
        } catch (InterruptedException e) {
            throw new ZkLockException("interrupted when create dir");
        } catch (KeeperException e) {
            throw new ZkLockException(e);
        }
    }

    private void doLock() throws KeeperException, InterruptedException {
        boolean created = false;
        String createdNode = null;
        for (; ; ) {
            if (!created) {
                createdNode = zk.create(followerDir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                created = true;
            }
            List<String> children = zk.getChildren(lockDir, null);
            TreeSet<String> treeSet = new TreeSet<>(children);
            String first = lockDir + "/" + treeSet.first();
            if (createdNode.equals(first)) {
                // lock success
                synchronized (this) {
                    lockOwner = Thread.currentThread();
                    lockNode = createdNode;
                }
                return;
            }
            String needWatch = lockDir + "/" + treeSet.higher(createdNode);
            CountDownLatch latch = new CountDownLatch(1);
            Stat exists = zk.exists(needWatch, event -> {
                if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
                    latch.countDown();
                }
            });
            if (exists == null) {
                continue;
            }
            latch.await();
        }
    }

    public void unlock() throws KeeperException, InterruptedException {
        if (Thread.currentThread() == lockOwner) {
            synchronized (this) {
                lockOwner = null;
                Stat exists = zk.exists(lockNode, false);
                if (exists == null) {
                    return;
                }
                zk.delete(lockNode, exists.getVersion());
            }
        }
    }

}

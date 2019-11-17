package com.greetingtech.zk.lock;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * @author greetingtech
 * @date 2019-11-16
 */
public class Example {

    private static int count = 0;

    public static void main(String[] args) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Watcher watcher = event -> {
            boolean connectSuccess =
                    event.getState() == Watcher.Event.KeeperState.SyncConnected
                            && event.getType() == Watcher.Event.EventType.None
                            && event.getPath() == null;
            if (connectSuccess) {
                System.out.println("connect success");
                latch.countDown();
                return;
            }
            System.out.println(event);
        };
        try (ZooKeeper zk = new ZooKeeper("127.0.0.1", 10000, watcher)) {
            latch.await();
            ZkLockFactory factory = new ZkLockFactory(zk, "/lock");
            ZkLock lock = factory.createLock("/my-lock");

            final int threadCount = 16;
            CountDownLatch testLatch = new CountDownLatch(threadCount);
            for (int i = 0; i < threadCount; ++i) {
                test(lock, testLatch);
            }
            testLatch.await();
        }
    }

    private static void test(ZkLock lock, CountDownLatch latch) throws Exception {
        Thread thread = new Thread(() -> {
            lock.lock();
            try {
                try {
                    Thread.sleep(100);
                    ++count;
                    System.out.println("success " + count);
                } finally {
                    lock.unlock();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        });
        thread.start();
    }

}

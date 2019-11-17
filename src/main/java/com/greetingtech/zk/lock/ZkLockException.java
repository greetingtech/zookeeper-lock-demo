package com.greetingtech.zk.lock;

/**
 * @author greetingtech
 * @date 2019-11-16
 */
public class ZkLockException extends RuntimeException {

    public ZkLockException(String message) {
        super(message);
    }

    public ZkLockException(Exception e) {
        super(e);
    }

}

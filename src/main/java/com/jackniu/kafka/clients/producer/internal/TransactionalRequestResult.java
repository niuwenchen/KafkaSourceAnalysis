package com.jackniu.kafka.clients.producer.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public  final class TransactionalRequestResult {
    static final TransactionalRequestResult COMPLETE = new TransactionalRequestResult(new CountDownLatch(0));

    private final CountDownLatch latch;
    private volatile RuntimeException error = null;

    public TransactionalRequestResult() {
        this(new CountDownLatch(1));
    }

    private TransactionalRequestResult(CountDownLatch latch) {
        this.latch = latch;
    }

    public void setError(RuntimeException error) {
        this.error = error;
    }

    public void done() {
        this.latch.countDown();
    }

    public void await() {
        boolean completed = false;

        while (!completed) {
            try {
                latch.await();
                completed = true;
            } catch (InterruptedException e) {
                // Keep waiting until done, we have no other option for these transactional requests.
            }
        }

        if (!isSuccessful())
            throw error();
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        boolean success = latch.await(timeout, unit);
        if (!isSuccessful())
            throw error();
        return success;
    }

    public RuntimeException error() {
        return error;
    }

    public boolean isSuccessful() {
        return error == null;
    }

    public boolean isCompleted() {
        return latch.getCount() == 0L;
    }
}

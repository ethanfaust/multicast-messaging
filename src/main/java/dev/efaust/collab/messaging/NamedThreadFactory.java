package dev.efaust.collab.messaging;

import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory {
    private String threadName;

    public NamedThreadFactory(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(threadName);
        return thread;
    }
}

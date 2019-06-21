package netty.thread;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDefaultEventExecutorGroup {
    public static void main(String[] args) {
        EventExecutorGroup eventExecutorGroup = new DefaultEventExecutorGroup(2, new ThreadFactory() {
            AtomicInteger atomicInteger = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,String.format("Thread-%d",atomicInteger.getAndIncrement()));
            }
        });
        eventExecutorGroup.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("Test TestDefaultEventExecutorGroup "+Thread.currentThread().getName());
            }
        });
        eventExecutorGroup.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("Test TestDefaultEventExecutorGroup1 "+Thread.currentThread().getName());
            }
        });
        eventExecutorGroup.shutdownGracefully();
    }
}

package com.alibaba.rocketmq.broker;

/**
 * 测试shutdownHook功能:jvm关闭时执行的钩子
 */
public class TestShutdownHook {
    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("虚拟机关闭之前执行钩子线程");
            }
        }));
        while (true){

        }
    }
}

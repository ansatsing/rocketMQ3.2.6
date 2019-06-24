package com.alibaba.rocketmq.broker;

/**
 * 测试shutdownHook功能:jvm关闭时执行的钩子
 * 1-程序正常退出
 * 2-使用System.exit()
 * 3-终端使用Ctrl+C触发的中断
 * 4-系统关闭
 * 5-OutOfMemory宕机
 * 6-使用Kill pid命令干掉进程（注：在使用kill -9 pid时，是不会被调用的）
 *
 *  注意：idea环境debug虚拟机钩子线程需点击Exit按钮
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

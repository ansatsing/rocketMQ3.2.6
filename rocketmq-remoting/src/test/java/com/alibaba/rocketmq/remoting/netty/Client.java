package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class Client {
    public static void main(String[] args) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        NettyRemotingClient nettyRemotingClient = new NettyRemotingClient(nettyClientConfig);
        nettyRemotingClient.start();
        RemotingCommand requestCommand = RemotingCommand.createRequestCommand(0,null);
        requestCommand.setBody("我是rockemq client".getBytes());
        RemotingCommand response = nettyRemotingClient.invokeSync(Constant.SERVER_ADDR,requestCommand,3000);
        System.out.println(response);
    }
}

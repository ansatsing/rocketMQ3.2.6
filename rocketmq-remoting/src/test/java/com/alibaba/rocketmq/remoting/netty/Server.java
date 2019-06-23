package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Executors;

/**
 * 服务器端app
 */
public class Server {
    public static void main(String[] args) {
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(Constant.SERVER_LINSTEN_PORT);
        NettyRemotingServer nettyRemotingServer = new NettyRemotingServer(nettyServerConfig);
        nettyRemotingServer.registerProcessor(0, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                System.out.printf("收到来自%s的%d类型的消息:%s\n",ctx.channel().remoteAddress().toString(),0,new String(request.getBody()));
                RemotingCommand response = RemotingCommand.createResponseCommand(0,new String(request.getBody()));
                return response;
            }
        }, Executors.newCachedThreadPool());
        nettyRemotingServer.registerProcessor(1, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                System.out.printf("收到来自%s的%d类型的消息:%s\n",ctx.channel().remoteAddress().toString(),1,new String(request.getBody()));
                RemotingCommand response = RemotingCommand.createResponseCommand(0,new String(request.getBody()));
                return response;
            }
        }, Executors.newCachedThreadPool());
        nettyRemotingServer.start();
    }
}

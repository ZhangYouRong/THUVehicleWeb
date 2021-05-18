package com.tsinghua.kafka;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

public class ServerInitializer extends ChannelInitializer<SocketChannel> {
    public final static int SOCKET_READ_TIMEOUT_SECOND = 30;

    public ServerInitializer() {

    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        //设置read超时，write超时，all超时时间
        pipeline.addLast("timeout", new IdleStateHandler(SOCKET_READ_TIMEOUT_SECOND, 0, 0));
        //设置定制的解码器
        pipeline.addLast("decoder", new DemoDecoder());
        //设置定制的链接管理器，只处理数据上报时可以不用修改
        pipeline.addLast("session", new SessionHandler());
        //设置定制的超时处理器，只处理数据上报时可以不用修改
        pipeline.addLast("timeoutProcess", new TimeoutHandler());
    }

}
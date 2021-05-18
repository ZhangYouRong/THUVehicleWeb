package com.tsinghua.kafka;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class NettyServer implements Runnable {
    public final static String tag = "NettyServer";
    //
    public final static String IP = "0.0.0.0";		//服务端IP
    public final static int PORT = 15000;			//服务端端口

    public NettyServer() {

    }

    @Override
    public void run() {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            ServerInitializer initializer = new ServerInitializer();	//服务器初始化
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.WARN))
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(initializer);
            ChannelFuture f = b.bind(IP, PORT).sync();
            Logger.i(tag, "Server started, at: " + IP + ":" + PORT);
            f.channel().closeFuture().sync();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) {
        NettyServer nettyServer = new NettyServer();	//构造定制的netty服务器
        new Thread(nettyServer).start();				//启动之
    }
}


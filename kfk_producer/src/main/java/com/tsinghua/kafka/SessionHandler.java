package com.tsinghua.kafka;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class SessionHandler extends ChannelInboundHandlerAdapter {
    public final static String tag = "SessionHandler";

    public SessionHandler() {

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object in) throws Exception {
        Logger.d(tag, "data=" + (short)in);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        Logger.d(tag, "channelReadComplete, " + ctx.channel().remoteAddress().toString());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
//		global.addChannel(ctx.channel());
        Logger.d(tag, "handlerAdded, " + ctx.channel().remoteAddress().toString());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
//		global.removeChannel(ctx.channel());
        Logger.d(tag, "handlerRemoved, " + ctx.channel().remoteAddress().toString());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Logger.d(tag, "channelActive, " + ctx.channel().remoteAddress().toString());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Logger.d(tag, "channelInactive, " + ctx.channel().remoteAddress().toString());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // channel exception
        Logger.d(tag, "exceptionCaught, " + cause.getMessage());
    }

}

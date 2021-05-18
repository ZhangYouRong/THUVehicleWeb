package com.tsinghua.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

public class TimeoutHandler extends SimpleChannelInboundHandler<ByteBuf> {
    public final static String tag = "TimeoutHandler";

    public TimeoutHandler() {

    }

    @Override
    protected void channelRead0(ChannelHandlerContext arg0, ByteBuf arg1) throws Exception {
        Logger.d(tag, "nerver reached!!!");

    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            switch (e.state()) {
                case READER_IDLE: // read time out
                    Logger.d(tag, "READER_IDLE");
                    handleReaderIdle(ctx);
                    break;
                case WRITER_IDLE: // write time out
                    Logger.d(tag, "WRITER_IDLE");
                    handleWriterIdle(ctx);
                    break;
                case ALL_IDLE: // all time out
                    Logger.d(tag, "ALL_IDLE");
                    handleAllIdle(ctx);
                    break;
                default:
                    break;
            }
        }
    }

    protected void handleReaderIdle(ChannelHandlerContext ctx) {
        //
    }

    protected void handleWriterIdle(ChannelHandlerContext ctx) {
        //
    }

    protected void handleAllIdle(ChannelHandlerContext ctx) {
        //
    }
}
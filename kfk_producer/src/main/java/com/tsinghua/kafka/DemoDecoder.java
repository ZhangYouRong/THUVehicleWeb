package com.tsinghua.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DemoDecoder extends LengthFieldBasedFrameDecoder{
    public final static String tag = "DemoDecoder";
    //
    public final static int MAX_FRAME_LENGTH = 1024;	//消息最大长度，单位byte
    public final static int LENGTH_FIELD_OFFSET = 7;	//
    public final static int LENGTH_FIELD_LENGTH = 2;	//
    //
    public final static int CHECK_SUM_LENGTH = 1;		//

    public DemoDecoder() throws IOException {
        super(MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, CHECK_SUM_LENGTH, 0);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        Logger.d(tag, "message incoming" + ctx.channel().remoteAddress().toString());
        if (in == null) {		//空消息
            Logger.d(tag, "empty buffer");
            return null;
        } else {				//打印原始消息内容，16进制
            Logger.d(tag, "[RawMessage]" + ByteBufUtil.hexDump(in));
        }
        ByteBuf bb = (ByteBuf) super.decode(ctx, in);	//使用上述配置解码
        if (bb == null) {
            Logger.d(tag, "message error");
            return null;
        }
        ByteBuf all = bb.readBytes(bb.readableBytes());

        // convert netty.ByteBuf to nio.byteBuffer
        int numReadBytes = all.readableBytes();
        System.out.println(numReadBytes);
        byte [] conBytes = new byte[numReadBytes];
        all.readBytes(conBytes);
        ByteBuffer conByteBuffer = ByteBuffer.allocate(conBytes.length);
        conByteBuffer.put(conBytes);

        all.resetReaderIndex();
        int len = all.readableBytes();	//获取消息长度
        Logger.d(tag, "[MessageSize]" + len);
        all.readInt();						//跳过SOI
        byte gid = all.readByte();			//读GID
        byte addr = all.readByte();			//
        byte cid = all.readByte();			//
        int infoLength = all.readShort();	//读info的长度
        int sn = all.readInt();				//读sn
        Logger.d(tag, "gid=" + gid + ", addr=" + addr + ", cid=" + cid + ", infoLength=" + infoLength + ", sn=" + sn);

        //write to kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteBufferSerializer");

        Producer<String, ByteBuffer> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>("test", "0", conByteBuffer));

        producer.close();

        return null;
    }

    public String bytesToHexString(byte[] bs) {		//byte数组转16进制字符串
        if (bs == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer(bs.length);
        String s;
        for (int i = 0; i < bs.length; i++) {
            s = Integer.toHexString(0xFF & bs[i]);
            if (s.length() < 2)
                sb.append(0);
            sb.append(s);
            sb.append(" ");
        }
        return sb.toString();
    }

    public String string2HexString(String str) {	//字符串转16进制字符串
        StringBuffer sb = new StringBuffer();
        String s;
        for (int i = 0; i < str.length(); i++) {
            int ch = (int) str.charAt(i);
            s = Integer.toHexString(ch);
            if (s.length() < 2)
                sb.append(0);
            sb.append(s + " ");
        }
        return sb.toString();
    }
}


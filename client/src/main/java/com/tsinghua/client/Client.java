package com.tsinghua.client;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client {
    public final static String IP_ADDRRESS = "127.0.0.1";
    public final static int PORT = 15000;

    private Socket socket;
    private InputStream in;
    private OutputStream out;
    //
    PackageBuilder pb = PackageBuilder.getInstance();

    public static void main(String[] args) {
        new Client(IP_ADDRRESS, PORT).start();    //启动全双工socket，localhost:15000
    }

    private Client(String address, int port) {
        try {
            socket = new Socket(address, port);        //初始化socket
            this.in = socket.getInputStream();        //获取输入流
            this.out = socket.getOutputStream();    //获取输出流
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        System.out.println("client start success");
    }

    public void start() {
//		Reader reader = new Reader();	//未处理server -> client
//		reader.start();
        Writer writer = new Writer();    //启动数据上传线程
        writer.start();
    }

    private class Writer extends Thread {

        @Override
        public void run() {
            try {
                for (int i = 1; i < 10; i++) {    //发送9个数据包，sn从1开始
                    //拼装数据包
                    byte[] pkg = pb.getPackage(i, (short) 2, 0L, 0L, (short) 0, (short) 0, (short) 0, (short) 0, (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x00);
                    pb.showPackage(pkg);        //打印数据16进制内容
                    out.write(pkg);                //发送
                    out.flush();                //刷新缓存
                    sleep(3000);
                }
                System.out.println("client socket close");
                close();
            } catch (IOException ioe) {
                System.out.println("socket closed");
            } catch (InterruptedException ie) {
                System.out.println("thread interrupted");
            } finally {
                close();
            }
        }
    }

    public void close() {
        try {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
            if (socket != null) {
                socket.close();
            }
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
package com.tsinghua.client;

import com.csvreader.CsvReader;

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
        new Client(IP_ADDRRESS, PORT).start();	//启动全双工socket，localhost:15000
    }

    private Client(String address, int port) {
        try {
            socket = new Socket(address, port);		//初始化socket
            this.in = socket.getInputStream();		//获取输入流
            this.out = socket.getOutputStream();	//获取输出流
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
        Writer writer = new Writer();	//启动数据上传线程
        writer.start();
    }

    private class Writer extends Thread {

        @Override
        public void run() {
            try {
                String filePath = "/root/artifacts/data/闽D00869D-20210501.csv";
                CsvReader csvReader = new CsvReader(filePath);

                // 读表头
                csvReader.readHeaders();
                while (csvReader.readRecord()) {
                    //int sn, short cm, double lat, double lng,
                    //float ca, float gs, short alt, short pa, byte gq, byte gf,
                    //byte iida, byte ns
                    int sn = Integer.parseInt(csvReader.get("seq"));
                    long ctm = Long.parseLong(csvReader.get("data_time"));
                    short cm = Short.parseShort(csvReader.get("control_mode"));
                    double lat = Double.parseDouble(csvReader.get("lat"));
                    double lng = Double.parseDouble(csvReader.get("lng"));
                    float ca = Float.parseFloat(csvReader.get("heading"));
                    float gs = Float.parseFloat(csvReader.get("gps_speed"));
                    short alt = Short.parseShort(csvReader.get("altitude"));
                    short pa = Short.parseShort(csvReader.get("pitch_angle"));

                    byte[] pkg = pb.getPackage(sn, ctm, cm, lat, lng, ca, gs, alt, pa, (byte)0x01, (byte)0x00, (byte)0x01, (byte)0x00);
                    pb.showPackage(pkg);		//打印数据16进制内容
                    out.write(pkg);				//发送
                    out.flush();				//刷新缓存
                    sleep(1000);
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
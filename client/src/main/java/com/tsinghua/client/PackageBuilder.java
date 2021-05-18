package com.tsinghua.client;

import java.nio.ByteBuffer;

public class PackageBuilder {

    public final static byte[] SOI = new byte[] {0x7e, 0x7e, 0x7e, 0x7e};
    public final static byte GID = 2;
    public final static byte ADDR = 2;
    //
    //public final static byte CID_OTA = 1;
    //public final static byte CID_LOG = 2;
    public final static byte CID_RTD = 16;
    public final static byte CID_DS = 57;
    //
    public static long timeStamp = 0L;
    //CID_RTD
    public static int serialNumber = 0;
    public static short controlModel = 2;
    public static long latitude = 0L;
    public static byte[] latiBytes = new byte[] {(byte)0xAA, (byte)0xAA, (byte)0xAA, (byte)0xAA, (byte)0xAA};
    public static long longitude = 0L;
    public static byte[] longBytes = new byte[] {(byte)0xAA, (byte)0xAA, (byte)0xAA, (byte)0xAA, (byte)0xAA};
    public static short courseAngle = 0;
    public static short gpsSpeed = 0;
    public static short altitude = 0;
    public static short pitchAngle = 0;
    public static byte gnssQuality = 1;
    public static byte gpsFault = 0;
    public static byte isInDesignatedArea = 1;
    public static byte networkStatus = 0;
    public static short LENGTH = 36;
    //CID_DS
    public static short distanceFromFront = 0;
    public static short speedOfFront = 0;

    private static PackageBuilder pb = null;

    private PackageBuilder() {

    }

    public static PackageBuilder getInstance() {
        if (null == pb) {
            pb = new PackageBuilder();
        }
        return pb;
    }

    public byte[] getPackage(int sn, short cm, long lat, long lng,
                             short ca, short gs, short alt, short pa, byte gq, byte gf,
                             byte iida, byte ns) {
        ByteBuffer info = ByteBuffer.allocate(LENGTH);
        info.putInt(sn);
        info.putLong(0L);
//		info.putLong(System.currentTimeMillis());
        info.putShort(cm);
        info.put(latiBytes);
        info.put(longBytes);
        info.putShort(ca);
        info.putShort(gs);
        info.putShort(alt);
        info.putShort(pa);
        System.out.println(info.position());
        info.put(gq);
        info.put(gf);
        info.put(iida);
        info.put(ns);
        System.out.println(info.position());

        ByteBuffer pkg = ByteBuffer.allocate(46);
        pkg.put(SOI);
        pkg.put(GID);
        pkg.put(ADDR);
        pkg.put(CID_RTD);
        pkg.putShort(LENGTH);
        pkg.put(info.array());

        byte[] cs = checkSum(pkg.array(), 1);
        pkg.put(cs[0]);
        return pkg.array();
    }

    private byte[] checkSum(byte[] msg, int length) {
        long mSum = 0;
        byte[] mByte = new byte[length];
        for (byte byteMsg : msg) {
            long mNum = ((long)byteMsg >= 0) ? (long)byteMsg : ((long)byteMsg + 256);
            mSum += mNum;
        }
        for (int liv_Count = 0; liv_Count < length; liv_Count++) {
            mByte[length - liv_Count - 1] = (byte)(mSum >> (liv_Count * 8) & 0xff);
        }
        return mByte;
    }

    public void showPackage(byte[] bs) {
        System.out.println("package size = " + bs.length);
        System.out.println(binaryToHexString(bs));
    }

    private final static String HEX_STRING = "0123456789ABCDEF";
    public static String binaryToHexString(byte[] bytes){
        String result = "";
        String hex = "";
        for(int i=0; i<bytes.length; i++) {
            hex = String.valueOf(HEX_STRING.charAt((bytes[i]&0xF0)>>4));
            hex += String.valueOf(HEX_STRING.charAt(bytes[i]&0x0F));
            result = result + hex + " ";
        }
        return result;
    }
}

package com.tsinghua.kafka;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
    public final static int LEVEL_DEBUG = 10;
    public final static int LEVEL_INFO = 20;
    public final static int LEVEL_ERROR = 30;
    //
    static int level = 0;
    static DateTimeFormatter formatter;
    static {
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        level = 0;
    }

    public static void d(String tag, byte[] buffer) {
        LocalDateTime time = LocalDateTime.now();
        System.out.print("<D><" + tag + "><" + formatter.format(time) + ">message buffer = ");
        String hex;
        for (int i = 0; i < buffer.length; i++) {
            hex = Integer.toHexString(buffer[i] & 0xFF);
            hex = hex.toUpperCase();
            if (hex.length() == 1) {
                hex = "0" + hex;
            }
            System.out.print(hex.toUpperCase() + " ");
        }
        System.out.println();
    }
    public static void d(String tag, String s) {
        if (10 > level) {
            w("D", tag, s);
        }
    }
    public static void i(String tag, String s) {
        if (20 > level) {
            w("I", tag, s);
        }
    }
    public static void e(String tag, String s) {
        if (30 > level) {
            w("E", tag, s);
        }
    }

    private static void w(String level, String tag, String s) {
        LocalDateTime time = LocalDateTime.now();
        System.out.println("<" + level + "><" + tag + "><" + formatter.format(time) + ">" + s);
    }

}


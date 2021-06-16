package com.tsinghua.kafka;

import org.apache.kafka.clients.consumer.*;

import java.nio.ByteBuffer;
import java.sql.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.Date;



public class MysqlConsumer
{
    //kafka initialization
    private static Properties props = new Properties();
    private KafkaConsumer<String, ByteBuffer> consumer;
    //mysql initialization
    static final String driverName = "com.mysql.cj.jdbc.Driver";
    static final String userName = "root";
    static final String userPwd = "THUsvm211!";

    static String dbURL= null;
    static final String dbURL_pre = "jdbc:mysql://localhost:3306/";
    static final String dbURL_suf = "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    static final int mysql_insert_unit=100;

    Connection conn=null;

    MysqlConsumer(String url)
    {
        props.put("bootstrap.servers", url);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteBufferDeserializer");
    }

    public boolean MysqlDBConnect(String database)
    {
        try
        {
            //注册JDBC驱动
            Class.forName(driverName);
            //尝试连接数据库
            dbURL=dbURL_pre+database+dbURL_suf;
            conn= DriverManager.getConnection(dbURL, userName, userPwd);
            System.out.println("连接数据库成功");
            return true;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.print("连接失败");
            return false;
        }
    }

    //在mysql里面创建一个新的表格
    public void TableCreate(String table_name)
    {
        //VARCHAR(1)代表一个汉字or一个英文字母
        String table = "CREATE TABLE IF NOT EXISTS "+table_name+
                "(id INT," +
                "t_name VARCHAR(5) NOT NULL," +
                "t_password VARCHAR(10) NOT NULL," +
                "sex VARCHAR(1) NOT NULL,"  +
                "description VARCHAR(10) NOT NULL," +
                "pic_url VARCHAR(20)," +
                "school_name VARCHAR(10) NOT NULL," +
                "regist_date DATETIME," +
                "remark VARCHAR(30)," +
                "PRIMARY KEY (id)" +
                ")ENGINE=InnoDB DEFAULT CHARSET=utf8;";
        try
        {
            conn.setAutoCommit(false);
            PreparedStatement pst =  conn.prepareStatement(" ");
            pst.addBatch(table);
            // 执行操作
            pst.executeBatch();
            // 提交事务
            conn.commit();
            System.out.println("表格初始化成功");
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

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

    public void GetMessage(String topic)
    {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        String insert_table="tb_D00869D_20210501";
        boolean receive_message=true;

        try
        {
            // 设置事务为非自动提交
            conn.setAutoCommit(false);
            // 比起st，pst会更好些
            PreparedStatement pst = (PreparedStatement) conn.prepareStatement(" ");//准备执行语句

            while (receive_message)
            {
                ConsumerRecords<String, ByteBuffer> records = consumer.poll(100);
                String prefix = "INSERT INTO " + insert_table + " (sn,ctm,cm,lat,lng,ca,gs,alt,pa) VALUES ";
                int counter = 0;
                StringBuffer suffix = new StringBuffer();

                for (ConsumerRecord<String, ByteBuffer> record : records) {
                    counter++;
                    byte [] SOI = new byte[4];
                    record.value().get(SOI);

                    byte GID = record.value().get();
                    byte ADDR = record.value().get();
                    byte CID_RTD = record.value().get();
                    short LENGTH = record.value().getShort();
                    int sn = record.value().getInt();
                    long ctm =  record.value().getLong();
                    short cm =  record.value().getShort();
                    double lat =  record.value().getDouble();
                    double lng = record.value().getDouble();
                    float ca = record.value().getFloat();
                    float gs = record.value().getFloat();
                    short alt = record.value().getShort();
                    short pa = record.value().getShort();

                    //转换为StringBuffer(sql语法)
                    suffix.append("('").
                            append(sn).append("','").
                            append(ctm).append("','").
                            append(cm).append("','").
                            append(lat).append("','").
                            append(lng).append("','").
                            append(ca).append("','").
                            append(gs).append("','").
                            append(alt).append("','").
                            append(pa).append("'").      //注意：最后这个没有逗号
                            append("),");

                    //每若干条数据打断并输入数据库一次
                    if (counter>mysql_insert_unit)
                        break;

                }

                if (counter != 0) //有新数据被拉取
                {
                    // 构建完整SQL
                    String sql = prefix + suffix.substring(0, suffix.length() - 1);
                    // 添加执行SQL
                    pst.addBatch(sql);
                    // 执行操作
                    pst.executeBatch();
                    // 提交事务
                    conn.commit();
                    System.out.printf("%d items insert into table %s%n",counter, insert_table);
                }
            }
            // 头等连接
            pst.close();
            conn.close();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }


    }

    public void CloseConsumer()  {
        consumer.close();
    }
}

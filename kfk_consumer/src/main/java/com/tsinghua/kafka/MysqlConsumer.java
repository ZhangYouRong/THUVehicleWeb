package com.tsinghua.kafka;

import org.apache.kafka.clients.consumer.*;
import java.sql.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.Date;



public class MysqlConsumer
{
    //kafka initialization
    private static Properties props = new Properties();
    private KafkaConsumer<String, String> consumer;
    //mysql initialization
    static final String driverName = "com.mysql.cj.jdbc.Driver";
    static final String userName = "root";
    static final String userPwd = "THUsvm211!";

    static String dbURL= null;
    static final String dbURL_pre = "jdbc:mysql://localhost:3306/";
    static final String dbURL_suf = "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    Connection conn=null;

    MysqlConsumer(String url)
    {
        props.put("bootstrap.servers", url);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
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

    public void Insert(String table_name)
        {
        // 开始时间
        Long begin = new Date().getTime();
        TableCreate(table_name);
        // sql前缀
        String prefix = "INSERT INTO "+table_name+" (id,t_name,t_password,sex,description,pic_url,school_name,regist_date,remark) VALUES ";
        try {
            // 保存sql后缀
            StringBuffer suffix = new StringBuffer();
            // 设置事务为非自动提交
            conn.setAutoCommit(false);
            // 比起st，pst会更好些
            PreparedStatement  pst = (PreparedStatement) conn.prepareStatement(" ");//准备执行语句
            // 外层循环，总提交事务次数
            for (int i = 1; i <= 100; i++) {
                suffix = new StringBuffer();
                // 第j次提交步长
                for (int j = 1; j <= 10000; j++) {
                    // 构建SQL后缀
                    suffix.append("('").append(i*10000+j).append("',") .append("'张有容','123456','男','教师','www.bbk.com','XX大学','2016-08-12 14:43:26','备注'),");

//                    suffix.append("('").append(UUID.randomUUID().toString()).append("','") .append(i*j).append("','123456','男','教师','www.bbk.com','XX大学','2016-08-12 14:43:26','备注'),");
                }
                // 构建完整SQL
                String sql = prefix + suffix.substring(0, suffix.length() - 1);
                // 添加执行SQL
                pst.addBatch(sql);
                // 执行操作
                pst.executeBatch();
                // 提交事务
                conn.commit();
                Long now = new Date().getTime();
                //System.out.println(i+" :"+(float)(now - begin) / 1000);
                // 清空上一次添加的数据
                suffix = new StringBuffer();
            }
            // 头等连接
            pst.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 结束时间
        Long end = new Date().getTime();
        // 耗时
        System.out.println("100万条数据插入花费时间 : " + (float)(end - begin) / 1000 + " s");
        System.out.println("插入完成");
    }

    public void GetMessage(String topic)
    {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        String insert_table="tb_teacher";
        boolean receive_message=true;

        try
        {
            // 设置事务为非自动提交
            conn.setAutoCommit(false);
            // 比起st，pst会更好些
            PreparedStatement pst = (PreparedStatement) conn.prepareStatement(" ");//准备执行语句

            while (receive_message)
            {
                ConsumerRecords<String, String> records = consumer.poll(100);
                String prefix = "INSERT INTO " + insert_table + " (id,t_name,t_password,sex,description,pic_url,school_name,regist_date,remark) VALUES ";
                int counter = 0;
                StringBuffer suffix = new StringBuffer();
                for (ConsumerRecord<String, String> record : records) {
                    suffix.append("(").append(record.value()).append("),");
                    counter++;
                    System.out.printf("key = %s, value = %s%n", record.key(), record.value());
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
                    System.out.printf("%d items insert into table %s ",counter, insert_table);
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
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

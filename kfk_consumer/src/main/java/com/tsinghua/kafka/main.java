package com.tsinghua.kafka;

public class main
{
    public static void main(String[] args)
    {
        MysqlConsumer MC = new MysqlConsumer("node1:9092");
        MC.MysqlDBConnect("zhangyr");
        while(true)
        {
            MC.GetMessage("test");
        }
//        测试由自身插入一个tb_teacher的表格
//        MC.Insert("tb_teacher");
    }
}

package com.tsinghua.kafka;

public class main
{
    public static void main(String[] args)
    {
        MysqlConsumer MC = new MysqlConsumer("node1:9092");
        MC.MysqlDBConnect("xiamen_bus");
        while(true)
        {
            MC.GetMessage("test");
        }
    }
}

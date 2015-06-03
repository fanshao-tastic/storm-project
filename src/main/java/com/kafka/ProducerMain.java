package com.kafka;

public class ProducerMain {

	/**
	 * kafka-producer入口函数
	 */
	public static void main(String[] args) {
		if(args.length < 1) {
			System.out.println("请输入需要导入的表名");
			System.out.println("例如：java -cp your.jar yourMainClass TableName");
		} else {
			MysqlProducer mysqlProducer = new MysqlProducer(args[0]);
//			mysqlProducer.setOrderBy("ID");
			mysqlProducer.execute();
		}
	}
}

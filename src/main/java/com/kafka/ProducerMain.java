package com.kafka;

public class ProducerMain {

	public static void main(String[] args) {
		MysqlProducer mysqlProducer = new MysqlProducer("VehicleData20100901");
		mysqlProducer.setOrderBy("ID");
		mysqlProducer.execute();
	}
}

package com.kafka;

import static org.junit.Assert.*;

import org.apache.log4j.chainsaw.Main;
import org.junit.Before;
import org.junit.Test;

public class testRun {

	
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testKafkaProducer() {
		Main.main(null);
	}

}

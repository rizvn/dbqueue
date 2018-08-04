package com.rizvn;

import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;

/**
 * Created by Riz
 */
public class ProducerTest {
  Producer producer;

  @Before
  public void setUp(){
    DataSource dataSource = TestUtils.buildDataSource();

    producer = new Producer(dataSource);
  }

  @Test
  public void createDbTest(){
    producer.createQueueTable();
  }

  @Test
  public void insertMessage(){
    producer.produce("topic1", "Hello world");
  }

}
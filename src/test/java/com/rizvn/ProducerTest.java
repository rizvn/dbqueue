package com.rizvn;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Riz
 */
public class ProducerTest {
  Producer producer;

  @Before
  public void setUp(){
    DataSource dataSource = new DataSource();
    dataSource.setUrl("jdbc:postgresql://localhost/queue_test");
    dataSource.setUsername("postgres");
    dataSource.setPassword("password");
    dataSource.setDefaultAutoCommit(false);

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
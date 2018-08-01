package com.rizvn;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by Riz
 */
public class ConsumerTest {

  Producer producer;
  DataSource dataSource;

  @Before
  public void setup(){
    dataSource = new DataSource();
    dataSource.setUrl("jdbc:postgresql://localhost/queue_test");
    dataSource.setUsername("postgres");
    dataSource.setPassword("password");
    dataSource.setDefaultAutoCommit(false);


    producer = new Producer(dataSource);
  }

  @Test
  public void handleMessage(){
    MessageHandler messageHandler = (message -> System.out.println("Consumed: "+  message.toString()));
    Consumer consumer = new Consumer("1", dataSource, messageHandler, 10, TimeUnit.MILLISECONDS);
    consumer.handleMessage(messageHandler);
  }

  @Test
  public void multipleConsumers() throws Exception{

    MessageHandler handler = (message -> message.toString());

    Consumer consumer1 = new Consumer("1", dataSource, handler, 10, TimeUnit.MILLISECONDS);
    Consumer consumer2 = new Consumer("2", dataSource, handler, 10, TimeUnit.MILLISECONDS);
    Consumer consumer3 = new Consumer("3", dataSource, handler, 10, TimeUnit.MILLISECONDS);
    Consumer consumer4 = new Consumer("4", dataSource, handler, 10, TimeUnit.MILLISECONDS);

    consumer1.start();
    consumer2.start();
    consumer3.start();
    consumer4.start();

    for(int i=0; ; i++){
      Thread.sleep(100);
      producer.produce("Hello world "+ i);
    }

  }
}
package com.rizvn;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Riz
 */
public class ConsumerTest {

  Producer producer;
  DataSource dataSource;

  @Before
  public void setup(){
    dataSource = TestUtils.buildDataSource();
    producer = new Producer(dataSource);
  }

  @Test
  public void handleMessage(){
    MessageHandler messageHandler = (message -> System.out.println("Consumed: "+  message.toString()));
    Consumer consumer = new Consumer("1", dataSource, "topic1", messageHandler, 10, TimeUnit.MILLISECONDS);
    consumer.handleMessage(messageHandler);
  }

  @Test
  public void createTable(){
    producer.createQueueTable();
  }

  @Test
  public void multipleConsumers() throws Exception{
    final List<Long> ids = new ArrayList<>();
    MessageHandler handler = (message -> ids.add(message.messageId));

    Consumer consumer1 = new Consumer("1", dataSource, "topic1", handler, 50, TimeUnit.MILLISECONDS);
    Consumer consumer2 = new Consumer("2", dataSource, "topic1", handler, 50, TimeUnit.MILLISECONDS);
    Consumer consumer3 = new Consumer("3", dataSource, "topic1", handler, 50, TimeUnit.MILLISECONDS);
    Consumer consumer4 = new Consumer("4", dataSource, "topic1", handler, 50, TimeUnit.MILLISECONDS);
    Unlocker unlocker = new Unlocker(dataSource, 100, TimeUnit.MILLISECONDS, 3l);

    consumer1.start();
    consumer2.start();
    consumer3.start();
    consumer4.start();

    unlocker.start();

    for(int i=0; i < 100; i++){
    //  Thread.sleep(100);
      producer.produce("topic1", "Hello world "+ i);
    }

    Thread.sleep(10000);

    Set<Long> noDupes = new HashSet<Long>(ids);
    List<Long> sortedList = new ArrayList<>(noDupes);
    Collections.sort(sortedList);


    Assert.assertEquals("Duplicates found", noDupes.size(), ids.size());

    for(int i =0; i< sortedList.size(); i++){
      Assert.assertEquals("Elements at position don't match",sortedList.get(i), ids.get(i));
    }

  }
}
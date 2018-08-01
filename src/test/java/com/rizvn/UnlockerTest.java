package com.rizvn;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Created by Riz
 */
public class UnlockerTest {

  DataSource dataSource;

  @Before
  public void setup(){
    dataSource = new DataSource();
    dataSource.setUrl("jdbc:postgresql://localhost/queue_test");
    dataSource.setUsername("postgres");
    dataSource.setPassword("password");
    dataSource.setDefaultAutoCommit(false);
    dataSource.setMaxActive(20);


  }


  @Test
  public void releaseOldLockedMessages() throws Exception {

    Producer producer = new Producer(dataSource);
    Unlocker unlocker = new Unlocker(dataSource, 100, TimeUnit.MILLISECONDS, 3l);
    unlocker.start();

    for(int i=0; ; i++){
      Thread.sleep(100);
   //   producer.produce("topic1", "Hello world "+ i);
    }
  }
}
package com.rizvn;


import javax.sql.DataSource;

/**
 * Created by Riz
 */
public class TestUtils {

  public static DataSource buildDataSource(){
    org.apache.tomcat.jdbc.pool.DataSource dataSource = new org.apache.tomcat.jdbc.pool.DataSource();
    dataSource.setUrl("jdbc:sqlserver://localhost:1433;databasename=queue_test");
    dataSource.setUsername("sa");
    dataSource.setPassword("passwordABC123");
    dataSource.setDefaultAutoCommit(false);
    dataSource.setMaxActive(20);
    return dataSource;
  }
}

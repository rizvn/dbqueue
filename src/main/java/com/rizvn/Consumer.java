package com.rizvn;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Riz
 */
public class Consumer {

  DataSource dataSource;
  String name;
  MessageHandler messageHandler;
  int pollingInterval;
  TimeUnit timeUnit;

  ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

  public Consumer(String name, DataSource dataSource, MessageHandler messageHandler, int pollingInterval, TimeUnit timeUnit) {
    this.name = name;
    this.dataSource = dataSource;
    this.pollingInterval = pollingInterval;
    this.timeUnit = timeUnit;
    this.messageHandler = messageHandler;
  }

  public void start(){
    scheduledExecutorService.scheduleWithFixedDelay(() -> handleMessage(messageHandler), 0, pollingInterval, timeUnit);
  }

  public void stop(){
    scheduledExecutorService.shutdown();
  }

  public void handleMessage(MessageHandler handler){
    try(Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      Message message = null;
      try(Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)){
        String fetch_sql = "SELECT * from queue_table where locked isnull order by time_added asc LIMIT 1 FOR UPDATE ";
        try(ResultSet rs = stmt.executeQuery(fetch_sql)){
          while(rs.next()) {
            rs.updateTimestamp("locked", Timestamp.valueOf(LocalDateTime.now()));
            rs.updateString("locked_by", name);

            message = new Message();
            message.setMessageId(rs.getLong("id"));
            message.setText(rs.getString("message"));
            System.out.println("Consumer: "+ name + " "+ message.toString());
            rs.updateRow();
          }
        }
      }

      connection.commit();

      if(message != null){
        try {
          handler.apply(message);
          deleteMessage(message);
        }
        catch (Exception ex){
          releaseMessage(message);
        }
      }
    }catch (Exception ex){
      throw new IllegalStateException(ex);
    }
  }


  protected void releaseMessage(Message message){
    try(Connection conn = dataSource.getConnection()){
      String sql = "update queue_table set locked  = NULL where id = ?";

      try(PreparedStatement statement = conn.prepareStatement(sql)) {
        statement.setLong(1, message.getMessageId());
        statement.execute();
      }
      catch (Exception ex){
        throw new IllegalStateException(ex);
      }
      conn.commit();
    }catch (Exception ex){
      throw new IllegalStateException(ex);
    }
  }


  protected void deleteMessage(Message message){
    try(Connection conn = dataSource.getConnection()){
      String sql = "delete from queue_table where id = ?";

      try(PreparedStatement statement = conn.prepareStatement(sql)) {
        statement.setLong(1, message.getMessageId());
        statement.execute();
      }
      catch (Exception ex){
        throw new IllegalStateException(ex);
      }
      conn.commit();
    }catch (Exception ex){
      throw new IllegalStateException(ex);
    }
  }

}

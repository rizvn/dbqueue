package com.rizvn;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;

/**
 * Created by Riz
 */
public class Producer {

  DataSource dataSource;

  public Producer(DataSource dataSource){
    this.dataSource = dataSource;
  }

  public void produce(String topic, String payload){
    try(Connection conn = dataSource.getConnection()){
      String sql = "insert into message_queue (topic, payload, time_added) values(?, ?, ?)";

      try(PreparedStatement statement = conn.prepareStatement(sql)) {
        statement.setString(1, topic);
        statement.setString(2, payload);
        statement.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
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

  public void createQueueTable(){
    try(Connection conn = dataSource.getConnection()){
      try(Statement statement = conn.createStatement()) {
        statement.execute("" +
        "   CREATE TABLE message_queue (           " +
        "    [id] bigint IDENTITY (1,1),   " +
        "    topic varchar(30) NOT NULL,      " +
        "    payload varchar(255) NOT NULL,    " +
        "    time_added datetime2,     " +
        "    locked datetime2,         " +
        "    locked_by varchar(30)    " +
        ")");


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

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

  public void produce(String message){
    try(Connection conn = dataSource.getConnection()){
      String sql = "insert into queue_table (message, time_added) values(?, ?)";

      try(PreparedStatement statement = conn.prepareStatement(sql)) {
        statement.setString(1, message);
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
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
        "   CREATE TABLE IF NOT EXISTS queue_table (           " +
        "    id  SERIAL PRIMARY KEY,   " +
        "    message TEXT NOT NULL,    " +
        "    time_added timestamp,     " +
        "    locked timestamp,         " +
        "    locked_by varchar(30),    " +
        "    expiration timestamp      " +
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

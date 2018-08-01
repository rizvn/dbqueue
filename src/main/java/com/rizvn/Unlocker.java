package com.rizvn;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Riz
 */
public class Unlocker {

  DataSource dataSource;
  int pollingInterval;
  TimeUnit timeUnit;
  Long timeoutSeconds;

  ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);


  public Unlocker(DataSource dataSource, int pollingInterval, TimeUnit timeUnit, Long timeoutSeconds) {
    this.dataSource = dataSource;
    this.pollingInterval = pollingInterval;
    this.timeUnit = timeUnit;
    this.timeoutSeconds = timeoutSeconds;
  }

  public void start(){
    scheduledExecutorService.scheduleWithFixedDelay(() -> releaseOldLockedMessages(), 0, pollingInterval, timeUnit);
  }

  public void stop(){
    scheduledExecutorService.shutdown();
  }


  protected void releaseOldLockedMessages(){
    try(Connection conn = dataSource.getConnection()){
      String sql = "update message_queue set locked = null where locked < ?";

      try(PreparedStatement statement = conn.prepareStatement(sql)) {
        statement.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now().minusSeconds(timeoutSeconds)));
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

package com.rizvn;

/**
 * Created by Riz
 */
@FunctionalInterface
public interface MessageHandler {
  void apply(Message message);
}

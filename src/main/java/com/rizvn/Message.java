package com.rizvn;

/**
 * Created by Riz
 */
public class Message {
  Long messageId;
  String payload;

  public Long getMessageId() {
    return messageId;
  }

  public void setMessageId(Long messageId) {
    this.messageId = messageId;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  @Override
  public String toString() {
    return "Message{" +
    "messageId=" + messageId +
    ", payload='" + payload + '\'' +
    '}';
  }
}

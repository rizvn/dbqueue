package com.rizvn;

/**
 * Created by Riz
 */
public class Message {
  Long messageId;
  String text;

  public Long getMessageId() {
    return messageId;
  }

  public void setMessageId(Long messageId) {
    this.messageId = messageId;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return "Message{" +
    "messageId=" + messageId +
    ", text='" + text + '\'' +
    '}';
  }
}

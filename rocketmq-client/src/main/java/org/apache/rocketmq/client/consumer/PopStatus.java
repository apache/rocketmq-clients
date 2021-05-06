package org.apache.rocketmq.client.consumer;

public enum PopStatus {
  FOUND,
  NO_NEW_MSG,
  POLLING_FULL,
  POLLING_NOT_FOUND,
  SERVICE_UNSTABLE,
  // Only for unit test.
  STATUS_FOR_TEST;
}

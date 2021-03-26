package org.apache.rocketmq.client.misc;

public enum BoundaryType {
  /** Indicate that lower boundary is expected. */
  LOWER("lower"),

  /** Indicate that upper boundary is expected. */
  UPPER("upper");

  private String name;

  BoundaryType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static BoundaryType getType(String name) {
    if (BoundaryType.UPPER.getName().equalsIgnoreCase(name)) {
      return UPPER;
    }
    return LOWER;
  }
}

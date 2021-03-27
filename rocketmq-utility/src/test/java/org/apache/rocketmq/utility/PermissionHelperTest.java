package org.apache.rocketmq.utility;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PermissionHelperTest {

  @Test
  public void testIsReadable() {
    // 111
    Assert.assertTrue(PermissionHelper.isReadable(7));
    // 100
    Assert.assertTrue(PermissionHelper.isReadable(4));
    // 010
    Assert.assertFalse(PermissionHelper.isReadable(2));
    // 001
    Assert.assertFalse(PermissionHelper.isReadable(1));
  }

  @Test
  public void testIsWriteable() {
    // 111
    Assert.assertTrue(PermissionHelper.isWriteable(7));
    // 100
    Assert.assertFalse(PermissionHelper.isWriteable(4));
    // 010
    Assert.assertTrue(PermissionHelper.isWriteable(2));
    // 001
    Assert.assertFalse(PermissionHelper.isWriteable(1));
  }

  @Test
  public void testIsInherited() {
    // 111
    Assert.assertTrue(PermissionHelper.isInherited(7));
    // 100
    Assert.assertFalse(PermissionHelper.isInherited(4));
    // 010
    Assert.assertFalse(PermissionHelper.isInherited(2));
    // 001
    Assert.assertTrue(PermissionHelper.isInherited(1));
  }
}

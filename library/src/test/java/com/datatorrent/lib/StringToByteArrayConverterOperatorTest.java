/**
 * Copyright (c) 2016 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.converter.StringToByteArrayConverterOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class StringToByteArrayConverterOperatorTest
{
  @Test
  public void testStringToByteArrayConversion()
  {
    StringToByteArrayConverterOperator testop = new StringToByteArrayConverterOperator();
    String test1 = "hello world with default encoding";
    byte[] bytes = test1.getBytes();
    CollectorTestSink<byte[]> testsink = new CollectorTestSink<>();
    TestUtils.setSink(testop.output, testsink);
    testop.beginWindow(0);
    testop.input.put(test1);
    testop.endWindow();

    Assert.assertEquals(1,testsink.collectedTuples.size());
    Assert.assertArrayEquals(bytes, testsink.collectedTuples.get(0));
  }
}

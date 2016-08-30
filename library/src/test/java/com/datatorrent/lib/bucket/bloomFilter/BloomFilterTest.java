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
package com.datatorrent.lib.bucket.bloomFilter;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Test for {@link BloomFilter}
 * <p>
 *
 */
public class BloomFilterTest
{
  @Test
  public void testBloomFilter()
  {
    BloomFilter<Integer> bf = new BloomFilter<Integer>(1000000, 0.01);

    for (int i = 0; i < 1000000; i++) {
      if (i % 2 == 0) {
        bf.add(i);
      }
    }

    int falsePositive = 0;
    for (int i = 0; i < 1000000; i++) {
      if (!bf.contains(i)) {
        Assert.assertTrue(i % 2 != 0);
      } else {
        // BF says its present
        if (i % 2 != 0) {
          // But was not there
          falsePositive++;
        }
      }
    }
    // Verify false positive prob
    double falsePositiveProb = falsePositive / 1000000.0;
    Assert.assertTrue(falsePositiveProb <= 0.01);
  }
}

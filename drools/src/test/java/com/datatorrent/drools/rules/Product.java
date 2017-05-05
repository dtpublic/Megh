/**
 * Copyright (c) 2017 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.drools.rules;

public class Product
{
  private long productId;
  private String type;
  private int discount;

  public Product(long productId, String type, int discount)
  {
    this.productId = productId;
    this.type = type;
    this.discount = discount;
  }

  public String getType()
  {
    return type;
  }

  public void setType(String type)
  {
    this.type = type;
  }

  public int getDiscount()
  {
    return discount;
  }

  public void setDiscount(int discount)
  {
    this.discount = discount;
  }

  public long getProductId()
  {
    return productId;
  }

  public void setProductId(long productId)
  {
    this.productId = productId;
  }

  @Override
  public String toString()
  {
    return "Product [productId=" + productId + ", type=" + type + ", discount=" + discount + "]";
  }
}

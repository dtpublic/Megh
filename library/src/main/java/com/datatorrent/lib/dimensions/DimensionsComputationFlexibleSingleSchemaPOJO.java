/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.gpo.GPOGetters;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * This operator performs dimensions computation on a POJO. See {@link AbstractDimensionsComputationFlexibleSingleSchema}
 * for description of how the dimensions computation is performed.
 * </p>
 * <p>
 * This operator is configured by by setting the getter expressions to use for extracting keys and values from input
 * POJOs.
 * </p>
 *
 * @displayName Dimension Computation POJO
 * @category Statistics
 * @tags event, dimension, aggregation, computation, pojo
 */
public class DimensionsComputationFlexibleSingleSchemaPOJO extends AbstractDimensionsComputationFlexibleSingleSchema<Object>
{
  /**
   * The array of getters to use to extract keys from input POJOs.
   */
  private transient GPOGetters gpoGettersKey;
  /**
   * The array of getters to use to extract values from input POJOs.
   */
  private transient GPOGetters gpoGettersValue;

  /**
   * Flag indicating whether or not getters need to be created.
   */
  private boolean needToCreateGetters = true;
  /**
   * This is a map from a key name (as defined in the {@link DimensionalConfigurationSchema}) to the getter
   * expression to use for that key.
   */
  private Map<String, String> keyToExpression;
  /**
   * This is a map from a value name (as defined in the {@link DimensionalConfigurationSchema}) to the getter
   * expression to use for that value.
   */
  private Map<String, String> aggregateToExpression;

  public DimensionsComputationFlexibleSingleSchemaPOJO()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void convert(InputEvent inputEvent, Object event)
  {
    if(needToCreateGetters) {
      needToCreateGetters = false;
      gpoGettersKey = createGetters(this.configurationSchema.getKeyDescriptorWithTime(),
                                    keyToExpression,
                                    event);
      gpoGettersValue = createGetters(this.configurationSchema.getInputValuesDescriptor(),
                                      aggregateToExpression,
                                      event);
    }

    GPOUtils.copyPOJOToGPO(inputEvent.getKeys(), gpoGettersKey, event);
    GPOUtils.copyPOJOToGPO(inputEvent.getAggregates(), gpoGettersValue, event);
  }

  /**
   * This is a helper method which creates the getters to extract values from a pojo into a {@link GPOMutable}
   * object.
   * @param fieldsDescriptor The {@link FieldsDescriptor} of the {@link GPOMutable} object.
   * @param valueToExpression The map from field name to getter expression for the values that will be extracted
   * into a {@link GPOMutable} object.
   * @param event The input pojo.
   * @return {@link GPOGetters} used to extract values from an input POJO into a {@link GPOMutable} object.
   */
  @SuppressWarnings("unchecked")
  private GPOGetters createGetters(FieldsDescriptor fieldsDescriptor,
                                   Map<String, String> valueToExpression,
                                   Object event)
  {
    GPOGetters gpoGetters = new GPOGetters();
    Map<Type, List<String>> typeToFields = fieldsDescriptor.getTypeToFields();

    for(Map.Entry<Type, List<String>> entry: typeToFields.entrySet()) {
      Type inputType = entry.getKey();
      List<String> fields = entry.getValue();

      switch(inputType) {
        case BOOLEAN: {
          gpoGetters.gettersBoolean = (PojoUtils.GetterBoolean[])GPOUtils.createGetters(fields,
                                                                                        valueToExpression,
                                                                                        event.getClass(),
                                                                                        boolean.class,
                                                                                        GetterBoolean.class);

          break;
        }
        case STRING: {
          gpoGetters.gettersString = GPOUtils.createGettersString(fields,
                                                                  valueToExpression,
                                                                  event.getClass());

          break;
        }
        case CHAR: {
          gpoGetters.gettersChar = (PojoUtils.GetterChar[])GPOUtils.createGetters(fields,
                                                                                  valueToExpression,
                                                                                  event.getClass(),
                                                                                  char.class,
                                                                                  GetterChar.class);

          break;
        }
        case DOUBLE: {
          gpoGetters.gettersDouble = (PojoUtils.GetterDouble[])GPOUtils.createGetters(fields,
                                                                                      valueToExpression,
                                                                                      event.getClass(),
                                                                                      double.class,
                                                                                      GetterDouble.class);

          break;
        }
        case FLOAT: {
          gpoGetters.gettersFloat = (PojoUtils.GetterFloat[])GPOUtils.createGetters(fields,
                                                                                    valueToExpression,
                                                                                    event.getClass(),
                                                                                    float.class,
                                                                                    GetterFloat.class);
          break;
        }
        case LONG: {
          gpoGetters.gettersLong = (PojoUtils.GetterLong[])GPOUtils.createGetters(fields,
                                                                                  valueToExpression,
                                                                                  event.getClass(),
                                                                                  long.class,
                                                                                  GetterLong.class);

          break;
        }
        case INTEGER: {
          gpoGetters.gettersInteger = (PojoUtils.GetterInt[])GPOUtils.createGetters(fields,
                                                                                    valueToExpression,
                                                                                    event.getClass(),
                                                                                    int.class,
                                                                                    GetterInt.class);

          break;
        }
        case SHORT: {
          gpoGetters.gettersShort = (PojoUtils.GetterShort[])GPOUtils.createGetters(fields,
                                                                                    valueToExpression,
                                                                                    event.getClass(),
                                                                                    short.class,
                                                                                    GetterShort.class);

          break;
        }
        case BYTE: {
          gpoGetters.gettersByte = (PojoUtils.GetterByte[])GPOUtils.createGetters(fields,
                                                                                  valueToExpression,
                                                                                  event.getClass(),
                                                                                  byte.class,
                                                                                  GetterByte.class);

          break;
        }
        case OBJECT: {
          gpoGetters.gettersObject = GPOUtils.createGettersObject(fields,
                                                                  valueToExpression,
                                                                  event.getClass());

          break;
        }
        default: {
          throw new IllegalArgumentException("The type " + inputType + " is not supported.");
        }
      }
    }

    return gpoGetters;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  /**
   * @return the keyToExpression
   */
  public Map<String, String> getKeyToExpression()
  {
    return keyToExpression;
  }

  /**
   * @param keyToExpression the keyToExpression to set
   */
  public void setKeyToExpression(Map<String, String> keyToExpression)
  {
    this.keyToExpression = keyToExpression;
  }

  /**
   * @return the aggregateToExpression
   */
  public Map<String, String> getAggregateToExpression()
  {
    return aggregateToExpression;
  }

  /**
   * @param aggregateToExpression the aggregateToExpression to set
   */
  public void setAggregateToExpression(Map<String, String> aggregateToExpression)
  {
    this.aggregateToExpression = aggregateToExpression;
  }
}

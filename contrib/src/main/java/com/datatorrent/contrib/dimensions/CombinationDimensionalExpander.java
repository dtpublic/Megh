package com.datatorrent.contrib.dimensions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensionalExpander;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;

/**
 * This class verify the key value combination before adding to the result
 * @author bright
 *
 */
public class CombinationDimensionalExpander implements DataQueryDimensionalExpander
{
  private static final Logger LOG = LoggerFactory.getLogger(CombinationDimensionalExpander.class);
  
  protected CombinationFilter combinationFilter;
  protected final Map<String, Collection<Object>> seenKeyValues;
  protected CombinationValidator<String, Object> combinationValidator;
  
  public CombinationDimensionalExpander(Map<String, Collection<Object>> seenEnumValues)
  {
    this.seenKeyValues = Preconditions.checkNotNull(seenEnumValues);
  }

  public CombinationDimensionalExpander withCombinationFilter(CombinationFilter combinationFilter)
  {
    this.setCombinationFilter(combinationFilter);
    return this;
  }
  public CombinationDimensionalExpander withCombinationValidator(CombinationValidator<String,Object> combinationValidator)
  {
    this.setCombinationValidator(combinationValidator);
    return this;
  }

  @Override
  public List<GPOMutable> createGPOs(Map<String, Set<Object>> keyToValues,
                                     FieldsDescriptor fd)
  {
    //Unclean work around until helper method in FieldsDescriptor is added
    List<String> fields = Lists.newArrayList(fd.getFieldList());
    fields.remove(DimensionsDescriptor.DIMENSION_TIME);
    fields.remove(DimensionsDescriptor.DIMENSION_TIME_BUCKET);

    List<GPOMutable> results = Lists.newArrayList();

    if (fields.isEmpty()) {
      results.add(new GPOMutable(fd));
      return results;
    } else {
      for (String key : fields) {
        if (seenKeyValues.get(key).isEmpty() && keyToValues.get(key).isEmpty()) {
          return results;
        }
      }
    }

    createKeyGPOsHelper(keyToValues, fd, fields, null, results);
    LOG.info("Number of query: {}", results.size());
    
    return results;
  }


  protected void createKeyGPOsHelper(
                                   Map<String, Set<Object>> keyToValues,
                                   FieldsDescriptor fd,
                                   List<String> fields,
                                   GPOMutable gpo,
                                   List<GPOMutable> resultGPOs)
  {
    //set the value for empty set
    for(int i=0; i<fields.size(); ++i)
    {
      String key = fields.get(i);
      Set<Object> vals = keyToValues.get(key);
  
      if(vals.isEmpty()) {
        vals = Sets.newHashSet(seenKeyValues.get(key));
        keyToValues.put(key, vals);
      }
    }

    //cleanup
    if(combinationFilter != null)
      keyToValues = combinationFilter.filter(keyToValues);
    
    if(combinationValidator != null)
      fields = combinationValidator.orderKeys(fields);
    Map<String, Set<Object>> combinedKeyValues = Maps.newHashMap();
    createKeyGPOsWithCleanKeyValues(0, keyToValues, fd, fields, gpo, combinedKeyValues, resultGPOs);
  }
  
  protected void createKeyGPOsWithCleanKeyValues(int index,
      Map<String, Set<Object>> keyToValues,
      FieldsDescriptor fd,
      List<String> fields,
      GPOMutable gpo,
      Map<String, Set<Object>> combinedKeyValues,
      List<GPOMutable> resultGPOs)
  {
    String key = fields.get(index);
    Set<Object> vals = keyToValues.get(key);
    
    for (Object val : vals) {
      GPOMutable gpoKey;

      if(index == 0) {
        gpoKey = new GPOMutable(fd);
      } else {
        gpoKey = new GPOMutable(gpo);
      }
      
      if(combinationValidator != null)
      {
        //this value is invalid, no need to continue, try next value
        if(!combinationValidator.isValid(combinedKeyValues, key, val))
          continue;
      }
      
      gpoKey.setFieldGeneric(key, val);
            
      if (index == fields.size() - 1) {
        resultGPOs.add(gpoKey);
      } else {
        Set<Object> addedValues = null;
        if(combinationValidator != null)
        {
          //add this key value into 
          addedValues = combinedKeyValues.get(key);
          if(addedValues == null)
          {
            addedValues = Sets.newHashSet();
            combinedKeyValues.put(key, addedValues);
          }
          addedValues.add(val);
        }
        createKeyGPOsWithCleanKeyValues(index + 1, keyToValues, fd, fields, gpoKey, combinedKeyValues, resultGPOs);
        if(addedValues != null)
          addedValues.remove(val);
      }
    }
  }

  public CombinationFilter getCombinationFilter()
  {
    return combinationFilter;
  }

  public void setCombinationFilter(CombinationFilter combinationFilter)
  {
    this.combinationFilter = combinationFilter;
  }

  public CombinationValidator<String, Object> getCombinationValidator()
  {
    return combinationValidator;
  }

  public void setCombinationValidator(CombinationValidator<String, Object> combinationValidator)
  {
    this.combinationValidator = combinationValidator;
  }

  
}

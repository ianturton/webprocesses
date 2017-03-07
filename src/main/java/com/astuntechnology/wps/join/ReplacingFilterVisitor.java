package com.astuntechnology.wps.join;

import java.util.Set;
import java.util.TreeSet;

import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.visitor.DuplicatingFilterVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.FilterVisitor;
import org.opengis.filter.expression.PropertyName;

/**
 * A filter visitor to convert a join filter involving two featureTypes into a filter with the target
 * attributes replaced with literal values based on the provided feature.
 * 
 * so [atrib1 = atrib2] becomes ['1223' = atrib2] 
 * 
 * Also allow for filters using aliases such as 
 * 
 * [ strToLowerCase([a.STATE_NAME]) = strToLowerCase([STATE_NAME]) ] becomes
 * [ strToLowerCase("Illinois") = strToLowerCase([STATE_NAME]) ]
 * 
 * @author ian
 *
 */
public class ReplacingFilterVisitor extends DuplicatingFilterVisitor implements FilterVisitor {
  SimpleFeature feature;
  Set<String> names = new TreeSet<>();
  FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
  String alias="";
  public void setFeature(SimpleFeature f) {
    feature = f;
  }

  public void addFixedProperty(String name) {
      names.add(name);
  }

  /* (non-Javadoc)
   * @see org.geotools.filter.visitor.DuplicatingFilterVisitor#visit(org.opengis.filter.expression.PropertyName, java.lang.Object)
   * 
   * Replace any matching propertyNames with literals.
   * 
   */
  @Override
  public Object visit(PropertyName expression, Object extraData) {
    //if this expression matches the one we are fixing then replace it with the literal value
    String propertyName = expression.getPropertyName();
    if(names.contains(propertyName)) {
      //remove alias from expression if present
      if(!alias.isEmpty()) {
          if(propertyName.startsWith(alias)) {
              String unAliased = propertyName.substring(alias.length()+1);
              expression = ff.property(unAliased);
          }
      }
      
      Object value = expression.evaluate(feature);
      if(value!=null) {
        return ff.literal (value);
      }else {
        return super.visit(expression, extraData);
      }
    }else {
      return super.visit(expression, extraData);
    }

  }

  public void setAlias(String alias) {
    if(alias!=null) {
      this.alias = alias;
    }else {
      this.alias = "";
    }
    
  }
  
  
}

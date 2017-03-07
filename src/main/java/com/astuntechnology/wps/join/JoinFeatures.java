package com.astuntechnology.wps.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.Join;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.visitor.SimplifyingFilterVisitor;
import org.opengis.feature.Attribute;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;

import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.geotools.util.logging.Logging;

public class JoinFeatures {
  private static final Logger LOGGER = Logging.getLogger("com.ianturton.cookbook.processes.join.JoinFeatures");
  FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
  SimpleFeatureTypeBuilder ftBuilder = new SimpleFeatureTypeBuilder();

  public SimpleFeatureSource join(DataStore joinTarget, DataStore joinSource, Join join, String typeName) {
    try {

      SimpleFeatureCollection out = null;

      SimpleFeatureCollection joinFeatures = joinSource.getFeatureSource(typeName).getFeatures();
      SimpleFeatureCollection inputFeatures = joinTarget.getFeatureSource(join.getTypeName()).getFeatures();
      out = joint(join, joinFeatures, inputFeatures);
      return DataUtilities.source(out);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    return null;
  }

  /**
   * 
   * @param schema
   *          - the schema for the feature
   * @return - a simple feature with default values for all properties
   */
  private SimpleFeature generateDefaultFeature(SimpleFeatureType schema) {
    SimpleFeature defaultFeature = DataUtilities.template(schema, schema.getTypeName());
    // we have to do this as our nullable attributes will be null if not
    for (Property prop : defaultFeature.getProperties()) {
      Object defaultValue = DataUtilities.defaultValue(prop.getType().getBinding());
      defaultFeature.setAttribute(prop.getName(), defaultValue);
    }
    return defaultFeature;
  }

  /**
   * @param join
   *          - a Join object containing the filter used to make the join can
   *          contain an alias on the source
   * @param defaultFeature
   *          - a feature that contains default values for the joining features
   * @param joinFeatures
   *          - the features to be joined
   * @param inputFeatures
   *          - the features to be joined to
   */
  public SimpleFeatureCollection joint(Join join, SimpleFeatureCollection joinFeatures,
      SimpleFeatureCollection inputFeatures) {
    boolean removeOut = false;
    // do we want to output all the properties
    SimpleFeatureType schema = null;
    SimpleFeatureType jSchema = null;
    Filter outputFilter = join.getFilter();
    String[] outFilterNames = DataUtilities.attributeNames(outputFilter);
    List<PropertyName> properties = join.getProperties();
    boolean all = properties == null || properties.isEmpty();
    List<PropertyName> props = new ArrayList<>();
    if (!all) {
      // grab list of properties
      props = new ArrayList<>();
      props.addAll(properties);
      for (String n : outFilterNames) {
        PropertyName p = ff.property(n);
        if (!props.contains(p)) {
          props.add(p);
          removeOut = true;
        }
      }
    }
    List<SimpleFeature> out = new ArrayList<>();

    // extract PropertyNames from filter
    Filter filter = join.getJoinFilter();

    if (all) {
      schema = inputFeatures.getSchema();
    } else {
      schema = selectProperties(inputFeatures.getSchema(), props);
      List<AttributeDescriptor> descs = schema.getAttributeDescriptors();
      for (AttributeDescriptor desc : descs) {
        // stop attributes occurring twice if they are in both schema
        props.remove(ff.property(desc.getName().getLocalPart()));
      }
    }
    if (all) {
      jSchema = joinFeatures.getSchema();
    } else {
      jSchema = selectProperties(joinFeatures.getSchema(), props);

    }
    SimpleFeature defaultFeature = generateDefaultFeature(jSchema);
    String[] filterNames = DataUtilities.attributeNames(filter);
    String alias = join.getAlias();
    ArrayList<String> names = new ArrayList<>();
    List<String> attributeNames = Arrays.asList(DataUtilities.attributeNames(schema));
    if (alias != null && !alias.isEmpty()) {
      // if the alias is set then we only want to replace aliased names
      for (String name : filterNames) {
        if (name.startsWith(alias + ".")) {
          names.add(name);
        }
      }
    } else {
      for (String name : filterNames) {
        if (attributeNames.contains(name)) {
          names.add(name);
        }
      }
    }

    SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    builder.setName(schema.getName());
    builder.addAll(schema.getAttributeDescriptors());
    builder.addAll(jSchema.getAttributeDescriptors());
    SimpleFeatureType fSchema = builder.buildFeatureType();
    SimpleFeatureBuilder fBuilder = new SimpleFeatureBuilder(fSchema);
    SimplifyingFilterVisitor simplifying = new SimplifyingFilterVisitor();
    try (SimpleFeatureIterator features = inputFeatures.features()) {
      ReplacingFilterVisitor visitor = new ReplacingFilterVisitor();
      visitor.setAlias(join.getAlias());
      for (String name : names) {
        visitor.addFixedProperty(name);
      }
      while (features.hasNext()) {
        SimpleFeature f = features.next();
        if (all) {
          fBuilder.addAll(f.getAttributes());
        } else {
          for (PropertyName pn : properties) {
            String propertyName = pn.getPropertyName();

            AttributeDescriptor descriptor = schema.getDescriptor(propertyName);

            if (descriptor != null) {

              Object attribute = f.getAttribute(propertyName);
              if (attribute != null) {
                fBuilder.set(propertyName, attribute);
              }
            }
          }
          if (removeOut) {// need an extra attribute
            for (String propertyName : outFilterNames) {

              AttributeDescriptor descriptor = schema.getDescriptor(propertyName);

              if (descriptor != null) {

                Object attribute = f.getAttribute(propertyName);
                if (attribute != null) {
                  fBuilder.set(propertyName, attribute);
                }
              }
            }
          }
        }
        Filter localFilter = join.getJoinFilter();
        visitor.setFeature(f);
        localFilter = (Filter) localFilter.accept(visitor, ff);
        localFilter = (Filter) localFilter.accept(simplifying, ff);
        // System.out.println(localFilter);

        // We are doing 1:1 matches only - so pick the first matching feature
        // (and hope for the best)

        SimpleFeature match = DataUtilities.first(joinFeatures.subCollection(localFilter));
        if (match != null) {
          if (all) {
            fBuilder.addAll(match.getAttributes());
          } else {
            for (PropertyName pn : properties) {
              String propertyName = pn.getPropertyName();
              AttributeDescriptor descriptor = jSchema.getDescriptor(propertyName);
              AttributeDescriptor descriptor2 = schema.getDescriptor(propertyName);
              if (descriptor != null && descriptor2 == null) {
                Object attribute = match.getAttribute(propertyName);
                if (attribute != null) {
                  fBuilder.set(propertyName, attribute);
                }
              }
            }
          }

        } else {
          if (all) {
            fBuilder.addAll(defaultFeature.getAttributes());
          } else {
            for (PropertyName pn : props) {
              String propertyName = pn.getPropertyName();
              AttributeDescriptor descriptor = jSchema.getDescriptor(propertyName);

              if (descriptor != null) {
                Object attribute = defaultFeature.getAttribute(propertyName);
                if (attribute != null) {
                  fBuilder.set(propertyName, attribute);
                }
              }
            }
            if (removeOut) {// need an extra attribute
              for (String propertyName : outFilterNames) {

                AttributeDescriptor descriptor = schema.getDescriptor(propertyName);

                if (descriptor != null) {

                  Object attribute = f.getAttribute(propertyName);
                  if (attribute != null) {
                    fBuilder.set(propertyName, attribute);
                  }
                }
              }
            }

          }
          LOGGER.fine("No match for " + localFilter);
        }
        out.add(fBuilder.buildFeature(f.getID()));
      }
    }
    if (outputFilter != null) {
      SimpleFeatureCollection subCollection = DataUtilities.collection(out).subCollection(outputFilter);

      if (!removeOut) {
        return subCollection;
      } else {

        String[] pNames = new String[properties.size()];
        int i = 0;
        for (PropertyName p : properties) {

          pNames[i++] = p.getPropertyName();
        }
        try {
          ArrayList<SimpleFeature> out2 = new ArrayList<>();
          SimpleFeatureType nSchema = DataUtilities.createSubType(fSchema, pNames);
          try (SimpleFeatureIterator itr = subCollection.features()) {
            while (itr.hasNext()) {
              SimpleFeature nf = DataUtilities.reType(nSchema, itr.next());
              out2.add(nf);
            }
          }
          return DataUtilities.collection(out2);
        } catch (SchemaException e) {
          throw new RuntimeException(e);
        }
      }
    } else {
      return DataUtilities.collection(out);
    }
  }

  /**
   * @param inputFeatures
   * @param props
   * @param schema
   * @return
   */
  private SimpleFeatureType selectProperties(SimpleFeatureType inSchema, List<PropertyName> props) {
    SimpleFeatureType schema = null;
    List<PropertyName> list = DataUtilities.addMandatoryProperties(inSchema, props);
    String[] propNames = new String[list.size()];
    int i = 0;
    for (PropertyName n : list) {
      propNames[i++] = n.getPropertyName();
    }
    try {
      schema = DataUtilities.createSubType(inSchema, propNames);
    } catch (SchemaException e) {
      throw new IllegalArgumentException(e);
    }
    return schema;
  }

}

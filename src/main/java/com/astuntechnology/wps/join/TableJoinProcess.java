package com.astuntechnology.wps.join;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;


import javax.xml.namespace.QName;
import javax.xml.parsers.ParserConfigurationException;

import org.eclipse.emf.common.util.EList;
import org.geotools.data.Join;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.wfs.v2_0.WFSConfiguration;
import org.geotools.xml.Parser;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;
import org.opengis.util.InternationalString;

import org.xml.sax.SAXException;

import net.opengis.wfs20.QueryType;

public class TableJoinProcess extends StaticMethodsProcessFactory {
  static final private FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
  public TableJoinProcess() {
    super(Text.text("TableJoin"), "TableJoin", TableJoinProcess.class);
  }

  public TableJoinProcess(InternationalString title, String namespace, Class<?> targetClass) {
    super(title, namespace, targetClass);
    // TODO Auto-generated constructor stub
  }

  @DescribeProcess(title = "Table Join", description = "Join two FeatureCollections based on a Join (including optional alias)")
  @DescribeResult(description = "the two collections joined together.")

  static public SimpleFeatureCollection joinTables(
      @DescribeParameter(name = "target", description = "the collection to be joined to", min = 1) SimpleFeatureCollection target,
      @DescribeParameter(name = "source", description = "the collection to be joined", min = 1, meta = {
          "mimeTypes=text/csv" }) SimpleFeatureCollection joinSource,
      @DescribeParameter(name = "join", description = "the Join element that describes the relationship between the collections", min = 1, meta = {
          "mimeTypes=text/xml" }) String joinXML) {
    Join join = null;
    join = parseJoinXML(joinXML);

    if (target == null) {
      throw new IllegalArgumentException("target can not be null");
    }
    if (joinSource == null) {
      throw new IllegalArgumentException("source can not be null");
    }
    if (join == null || join.getFilter() == null) {
      throw new IllegalArgumentException("join can not be null");
    }

    JoinFeatures joiner = new JoinFeatures();
    SimpleFeatureCollection out = joiner.joint(join, joinSource, target);
    
    return out;

  }

  @DescribeProcess(title = "Table Join", description = "Join two FeatureCollections based on a Join (including optional alias)")
  @DescribeResult(description = "the two collections joined together.")

  static public SimpleFeatureCollection simpleJoinTables(
      @DescribeParameter(name = "target", description = "the collection to be joined to", min = 1) SimpleFeatureCollection target,
      @DescribeParameter(name = "source", description = "the collection to be joined", min = 1) SimpleFeatureCollection joinSource,
      @DescribeParameter(name = "joinfilter", description = "the filter that describes the relationship between the collections", min = 1) String cql,
      @DescribeParameter(name = "alias", description = "the alias used in the filter for the target collection", min = 0) String alias,
      @DescribeParameter(name = "propertyNames", description = "comma seperated list of attributes to return", min = 0) String propertiesList,
      @DescribeParameter(name = "outputfilter", description = "filter to reduce features to return", min = 0) String outCQL
      ) {
    Filter filter;
    try {
      filter = ECQL.toFilter(cql);
    } catch (CQLException e) {
      throw new IllegalArgumentException(e);
    }
    Join join = new Join(target.getSchema().getTypeName(), filter);
    if(alias!=null && !alias.isEmpty()) {
      join.alias(alias);
    }
    if(propertiesList!=null && !propertiesList.isEmpty()) {
      String[] props = propertiesList.split(",");
      ArrayList<PropertyName> names = new ArrayList<>();
      for(String p:props) {
        if(!p.isEmpty()) {
          names.add(ff.property(p));
        }
      }
      if(join.getProperties() == null) {
        join.setProperties(names);
      } else {
        join.getProperties().addAll(names);
      }
    }
    if(outCQL!=null && !outCQL.isEmpty()) {
      try {
        join.setFilter(ECQL.toFilter(outCQL));
      } catch (CQLException e) {
        throw new IllegalArgumentException(e);
      }
    }
    if (joinSource == null) {
      throw new IllegalArgumentException("source can not be null");
    }
    if (join == null || join.getFilter() == null) {
      throw new IllegalArgumentException("join can not be null");
    }

    JoinFeatures joiner = new JoinFeatures();
    SimpleFeatureCollection out = joiner.joint(join, joinSource, target);

    return out;

  }

  private static Join parseJoinXML(String joinXML) {
    // System.out.println(joinXML);
    WFSConfiguration configuration = new org.geotools.wfs.v2_0.WFSConfiguration();
    Parser parser = new Parser(configuration);

    try {
      net.opengis.wfs20.QueryType q = (QueryType) parser.parse(new ByteArrayInputStream(joinXML.getBytes()));
      String typeName = null;
      EList<Object> typeNames = q.getTypeNames();
      for (Object o : typeNames) {
        typeName = ((QName) o).getLocalPart();
        break;
      }
      Filter filter = q.getFilter();

      Join j = new Join(typeName, filter);
      EList<String> aliases = q.getAliases();
      if (aliases.size() > 0) {
        for (String alias : aliases) {
          j.setAlias(alias);
          break;
        }
      }
     
      return j;
    } catch (IOException | SAXException | ParserConfigurationException e) {
      throw new IllegalArgumentException(e);
    }

  }
}

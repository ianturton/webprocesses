package com.astuntechnology.wps.join;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.Join;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.test.TestData;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;

public class JoinFeaturesTest {

  private DataStore statesDS;
  private DataStore stateIncDS;
  private DataStore statePayDS;
  private DataStore stateUnempDS;
  private final static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

  @Before
  public void setup() throws IOException {

    Map<String, Object> params = new HashMap<>();
    params.put("url", org.geotools.TestData.url("shapes/statepop.shp"));
    statesDS = DataStoreFinder.getDataStore(params);
    assertNotNull(statesDS);
    File state_inc = TestData.file(this, "states_income.csv");
    
    File state_pay = TestData.file(this, "staterentalpymnts0310.csv");
    File state_unemp = TestData.file(this, "unemply-4-16.csv");
    Map<String, Object> params2 = new HashMap<>();
    params2.put("url", DataUtilities.fileToURL(state_inc));
    stateIncDS = DataStoreFinder.getDataStore(params2);
    
    params2.put("url", DataUtilities.fileToURL(state_pay));
    statePayDS = DataStoreFinder.getDataStore(params2);
    assertNotNull(statePayDS);
    
    params2.put("url", DataUtilities.fileToURL(state_unemp));
    stateUnempDS = DataStoreFinder.getDataStore(params2);
    assertNotNull(stateUnempDS);

    
  }

  @Test
  public void testSimpleJoin() throws CQLException, IOException {
    String name = statesDS.getTypeNames()[0];
    SimpleFeatureSource source = statesDS.getFeatureSource(name);

    String name2 = stateIncDS.getTypeNames()[0];
    SimpleFeatureSource source2 = stateIncDS.getFeatureSource(name2);
    //System.out.println(source2.getSchema());
    JoinFeatures joiner = new JoinFeatures();

    Join j = new Join(statesDS.getSchema(name).getTypeName(), ECQL.toFilter("\"STATE_NAME\" = \"State\""));
    //j.setAlias("a");
    // SimpleFeatureSource ns = me.join(source, source2,
    // CQL.toFilter("\"STATE_NAME\" = strCapitalize(\"State\")"), null);
    SimpleFeatureSource ns = joiner.join(statesDS, stateIncDS, j, name2);
 
    
    Query q = new Query(ns.getName().getLocalPart());
    String state_name = "STATE_NAME";
    String persons = "PERSONS";
    String workers = "WORKERS";
    String inc80 = "inc1980";
    String inc12 = "inc2012";
 
    /*
     * Query q = new DefaultQuery(ns.getSchema().getTypeName(), Filter.INCLUDE,
     * new String[] { state_name, persons, workers ,inc80,inc12});
     */

    SortBy[] sortBy = new SortBy[1];
    sortBy[0] = joiner.ff.sort(inc80, SortOrder.DESCENDING);
    q.setSortBy(sortBy);

    SimpleFeature first = DataUtilities.first(ns.getFeatures(q));
    assertEquals("District of Columbia", first.getAttribute(state_name));
    assertEquals(12251, ((Integer)first.getAttribute("inc1980")).intValue());
    
    sortBy[0] = joiner.ff.sort(inc80, SortOrder.ASCENDING);
    q.setSortBy(sortBy);    
    
    SimpleFeature last = DataUtilities.first(ns.getFeatures(q));
    assertEquals("Mississippi", last.getAttribute(state_name));
    assertEquals(6573, ((Integer)last.getAttribute(inc80)).intValue());
 
    


  }

  @Test
  public void testFIPSJoin() throws CQLException, IOException {
    // Join with FIP codes (note need to add 0 to force numeric conversion!)
    
    String name = statesDS.getTypeNames()[0];
    SimpleFeatureSource source = statesDS.getFeatureSource(name);

    String name2 = statePayDS.getTypeNames()[0];
    SimpleFeatureSource source2 = statePayDS.getFeatureSource(name2);
    //System.out.println(source2.getSchema());
    JoinFeatures joiner = new JoinFeatures();
    //NOTE that STATE_FIPS is a string (some start with 0) 
    Join j = new Join(statesDS.getSchema(name).getTypeName(), ECQL.toFilter("parseInt(\"STATE_FIPS\") = \"STFIP\""));

    SimpleFeatureSource ns = joiner.join(statesDS, statePayDS, j, name2);
 
    
    Query q = new Query(ns.getName().getLocalPart());
    String state_name = "STATE_NAME";
    String fy2009= "FY_2009";

    SortBy[] sortBy = new SortBy[1];
    sortBy[0] = joiner.ff.sort(fy2009, SortOrder.DESCENDING);
    q.setSortBy(sortBy);
    
    /*Comparator<SimpleFeature> comp = DataUtilities.sortComparator(sortBy[0]);
    List<SimpleFeature> list = DataUtilities.list(ns.getFeatures());
    Comparators.verifyTransitivity(comp, list);*/

    SimpleFeature first = DataUtilities.first(ns.getFeatures(q));
    assertEquals("Maryland", first.getAttribute(state_name));
    assertEquals(128.35, ((Double)first.getAttribute(fy2009)).doubleValue(),0.00001);
    
    sortBy[0] = joiner.ff.sort(fy2009, SortOrder.ASCENDING);
    q.setSortBy(sortBy);    
    
    List<SimpleFeature> list = DataUtilities.list(ns.getFeatures(q));
    SimpleFeature nevada = list.get(2);
    assertEquals("Nevada", nevada.getAttribute(state_name));
    assertEquals(16.72, ((Double)nevada.getAttribute(fy2009)).doubleValue(),0.00001);
 
    
    /*try (SimpleFeatureIterator itr = ns.getFeatures(q).features()) {
      while (itr.hasNext()) {
        SimpleFeature f = itr.next();
        System.out.println(String.format("%-20s", f.getAttribute(state_name).toString()) + "\t"
            + f.getAttribute(fy2009)  );
      }
    }*/

  }
  
  @Test
  public void testProperties() throws IOException, CQLException {
    String name = statesDS.getTypeNames()[0];
    SimpleFeatureSource source = statesDS.getFeatureSource(name);

    String name2 = statePayDS.getTypeNames()[0];
    SimpleFeatureSource source2 = statePayDS.getFeatureSource(name2);
    //System.out.println(source2.getSchema());
    JoinFeatures joiner = new JoinFeatures();
    //NOTE that STATE_FIPS is a string (some start with 0) 
    Join j = new Join(statesDS.getSchema(name).getTypeName(), ECQL.toFilter("parseInt(\"STATE_FIPS\") = \"STFIP\""));
    ArrayList<PropertyName> props = new ArrayList<>();
    props.add(ff.property("FY_2007"));
    props.add(ff.property("FY_2008"));
    props.add(ff.property("FY_2009"));
    props.add(ff.property("STATE_NAME"));
    props.add(ff.property("STATE_FIPS"));
    props.add(ff.property("the_geom"));
    j.setProperties(props);
    SimpleFeatureSource ns = joiner.join(statesDS, statePayDS, j, name2);
    SimpleFeatureType schema = ns.getSchema();
    assertEquals("Wrong number of attributes",6,schema.getAttributeCount());
    Query q = new Query(ns.getName().getLocalPart());
    String state_name = "STATE_NAME";
    String fy2009= "FY_2009";

    SortBy[] sortBy = new SortBy[1];
    sortBy[0] = joiner.ff.sort(fy2009, SortOrder.DESCENDING);
    q.setSortBy(sortBy);
    
/*    try (SimpleFeatureIterator itr = ns.getFeatures(q).features()) {
      while (itr.hasNext()) {
        SimpleFeature f = itr.next();
        System.out
            .println(String.format("%-20s", f.getAttribute(state_name).toString()) + "\t" + f.getAttribute(fy2009));
         System.out.println(f); 
      }
    }*/
    SimpleFeature first = DataUtilities.first(ns.getFeatures(q));
    assertEquals("Maryland", first.getAttribute(state_name));
    assertEquals(128.35, ((Double)first.getAttribute(fy2009)).doubleValue(),0.00001);
    
    sortBy[0] = joiner.ff.sort(fy2009, SortOrder.ASCENDING);
    q.setSortBy(sortBy);    
    
    List<SimpleFeature> list = DataUtilities.list(ns.getFeatures(q));
    SimpleFeature nevada = list.get(2);
    assertEquals("Nevada", nevada.getAttribute(state_name));
    assertEquals(16.72, ((Double)nevada.getAttribute(fy2009)).doubleValue(),0.00001);
 
    for(int i=0;i<props.size();i++) {
      String expected = props.get(i).getPropertyName();
      assertNotNull("can't find property", nevada.getAttribute(expected));
    }
  }
  @Test
  public void testSimpleFilteredJoin() throws CQLException, IOException {
    String name = statesDS.getTypeNames()[0];
    SimpleFeatureSource source = statesDS.getFeatureSource(name);

    String name2 = stateIncDS.getTypeNames()[0];
    SimpleFeatureSource source2 = stateIncDS.getFeatureSource(name2);
    //System.out.println(source2.getSchema());
    JoinFeatures joiner = new JoinFeatures();

    Join j = new Join(statesDS.getSchema(name).getTypeName(), ECQL.toFilter("\"STATE_NAME\" = \"State\""));
    j.setFilter(ECQL.toFilter("\"inc2012\">40000"));
    //j.setAlias("a");
    // SimpleFeatureSource ns = me.join(source, source2,
    // CQL.toFilter("\"STATE_NAME\" = strCapitalize(\"State\")"), null);
    SimpleFeatureSource ns = joiner.join(statesDS, stateIncDS, j, name2);
//    try (SimpleFeatureIterator itr = ns.getFeatures().features()) {
//      while (itr.hasNext()) {
//        SimpleFeature f = itr.next();
//        System.out.println(
//            String.format("%-20s", f.getAttribute("STATE_NAME").toString()) + "\t" + f.getAttribute("inc2012"));
//        // System.out.println(f);
//      }
//    }
    assertEquals(26,ns.getFeatures().size());
    
    Query q = new Query(ns.getName().getLocalPart());
    String state_name = "STATE_NAME";
    String persons = "PERSONS";
    String workers = "WORKERS";
    String inc80 = "inc1980";
    String inc12 = "inc2012";
 
    SortBy[] sortBy = new SortBy[1];
    sortBy[0] = joiner.ff.sort(inc80, SortOrder.DESCENDING);
    q.setSortBy(sortBy);

    SimpleFeature first = DataUtilities.first(ns.getFeatures(q));
    assertEquals("District of Columbia", first.getAttribute(state_name));
    assertEquals(12251, ((Integer)first.getAttribute("inc1980")).intValue());
    
    sortBy[0] = joiner.ff.sort(inc80, SortOrder.ASCENDING);
    q.setSortBy(sortBy);    
    
    SimpleFeature last = DataUtilities.first(ns.getFeatures(q));
    assertEquals("South Dakota", last.getAttribute(state_name));
    assertEquals(7800, ((Integer)last.getAttribute(inc80)).intValue());
 
    


  }
  @Test
  public void testFilteredProperties() throws IOException, CQLException {
    String name = statesDS.getTypeNames()[0];
    SimpleFeatureSource source = statesDS.getFeatureSource(name);

    String name2 = statePayDS.getTypeNames()[0];
    SimpleFeatureSource source2 = statePayDS.getFeatureSource(name2);
    //System.out.println(source2.getSchema());
    JoinFeatures joiner = new JoinFeatures();
    //NOTE that STATE_FIPS is a string (some start with 0) 
    Join j = new Join(statesDS.getSchema(name).getTypeName(), ECQL.toFilter("parseInt(\"STATE_FIPS\") = \"STFIP\""));
    ArrayList<PropertyName> props = new ArrayList<>();
    props.add(ff.property("FY_2007"));
    props.add(ff.property("FY_2008"));
    props.add(ff.property("FY_2009"));
    props.add(ff.property("STATE_NAME"));
    props.add(ff.property("STATE_FIPS"));
    props.add(ff.property("the_geom"));
    j.setProperties(props);
    j.setFilter(ECQL.toFilter("\"FY_2009\" BETWEEN 40.0 AND 100.0"));
    SimpleFeatureSource ns = joiner.join(statesDS, statePayDS, j, name2);
    SimpleFeatureType schema = ns.getSchema();
    assertEquals("Wrong number of attributes",6,schema.getAttributeCount());
    
    assertEquals(25, ns.getFeatures().size());
    Query q = new Query(ns.getName().getLocalPart());
    String state_name = "STATE_NAME";
    String fy2009= "FY_2009";

    SortBy[] sortBy = new SortBy[1];
    sortBy[0] = joiner.ff.sort(fy2009, SortOrder.DESCENDING);
    q.setSortBy(sortBy);
    
    /*try (SimpleFeatureIterator itr = ns.getFeatures(q).features()) {
      while (itr.hasNext()) {
        SimpleFeature f = itr.next();
        System.out
            .println(String.format("%-20s", f.getAttribute(state_name).toString()) + "\t" + f.getAttribute(fy2009));
         
      }
    }*/
    SimpleFeature first = DataUtilities.first(ns.getFeatures(q));
    assertEquals("Vermont", first.getAttribute(state_name));
    assertEquals(95.53, ((Double)first.getAttribute(fy2009)).doubleValue(),0.00001);
    
    sortBy[0] = joiner.ff.sort(fy2009, SortOrder.ASCENDING);
    q.setSortBy(sortBy);    
    
    
    SimpleFeature nevada = DataUtilities.first(ns.getFeatures(q));
    assertEquals("Idaho", nevada.getAttribute(state_name));
    assertEquals(41.6, ((Double)nevada.getAttribute(fy2009)).doubleValue(),0.00001);
 
    for(int i=0;i<props.size();i++) {
      String expected = props.get(i).getPropertyName();
      assertNotNull("can't find property", nevada.getAttribute(expected));
    }
  }
  
  @Test
  public void testFilteredMissingProperties() throws IOException, CQLException {
    String name = statesDS.getTypeNames()[0];
    SimpleFeatureSource source = statesDS.getFeatureSource(name);

    String name2 = statePayDS.getTypeNames()[0];
    SimpleFeatureSource source2 = statePayDS.getFeatureSource(name2);
    //System.out.println(source2.getSchema());
    JoinFeatures joiner = new JoinFeatures();
    //NOTE that STATE_FIPS is a string (some start with 0) 
    Join j = new Join(statesDS.getSchema(name).getTypeName(), ECQL.toFilter("parseInt(\"STATE_FIPS\") = \"STFIP\""));
    ArrayList<PropertyName> props = new ArrayList<>();
    props.add(ff.property("FY_2007"));
    props.add(ff.property("FY_2008"));
    props.add(ff.property("FY_2009"));
    props.add(ff.property("STATE_NAME"));
    props.add(ff.property("STATE_FIPS"));
    props.add(ff.property("the_geom"));
    j.setProperties(props);
    j.setFilter(ECQL.toFilter("STATE_ABBR LIKE 'N%'"));
    SimpleFeatureSource ns = joiner.join(statesDS, statePayDS, j, name2);
    SimpleFeatureType schema = ns.getSchema();
    
    assertEquals("Wrong number of attributes",6,schema.getAttributeCount());
    
    assertEquals(8, ns.getFeatures().size());
    Query q = new Query(ns.getName().getLocalPart());
    String state_name = "STATE_NAME";
    String fy2009= "FY_2009";

    SortBy[] sortBy = new SortBy[1];
    sortBy[0] = joiner.ff.sort(fy2009, SortOrder.DESCENDING);
    q.setSortBy(sortBy);
    
    /*try (SimpleFeatureIterator itr = ns.getFeatures(q).features()) {
      while (itr.hasNext()) {
        SimpleFeature f = itr.next();
        System.out
            .println(String.format("%-20s", f.getAttribute(state_name).toString()) + "\t" + f.getAttribute(fy2009));
         
      }
    }*/
    SimpleFeature first = DataUtilities.first(ns.getFeatures(q));
    assertEquals("New York", first.getAttribute(state_name));
    assertEquals(68.45, ((Double)first.getAttribute(fy2009)).doubleValue(),0.00001);
    
    sortBy[0] = joiner.ff.sort(fy2009, SortOrder.ASCENDING);
    q.setSortBy(sortBy);    
    
    
    SimpleFeature nevada = DataUtilities.first(ns.getFeatures(q));
    assertEquals("Nevada", nevada.getAttribute(state_name));
    assertEquals(16.72, ((Double)nevada.getAttribute(fy2009)).doubleValue(),0.00001);
 
    for(int i=0;i<props.size();i++) {
      String expected = props.get(i).getPropertyName();
      assertNotNull("can't find property", nevada.getAttribute(expected));
    }
  }
}

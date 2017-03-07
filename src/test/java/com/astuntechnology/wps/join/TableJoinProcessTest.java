package com.astuntechnology.wps.join;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.Join;
import org.geotools.data.Parameter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.NameImpl;
import org.geotools.feature.collection.SortedSimpleFeatureCollection;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.process.ProcessException;
import org.geotools.process.ProcessExecutor;
import org.geotools.process.Processors;
import org.geotools.process.Progress;
import org.geotools.test.TestData;
import org.geotools.util.KVP;
import org.geotools.wfs.v2_0.WFS;
import org.geotools.wfs.v2_0.WFSConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import net.opengis.wfs20.QueryType;
import net.opengis.wfs20.Wfs20Factory;

public class TableJoinProcessTest {
  private DataStore statesDS;
  private DataStore stateIncDS;
  private DataStore statePayDS;
  private DataStore stateUnempDS;
  private DataStore placesDS;

  @Before
  public void setup() throws IOException {

    Map<String, Object> params = new HashMap<>();
    params.put("url", org.geotools.TestData.url("shapes/statepop.shp"));
    statesDS = DataStoreFinder.getDataStore(params);
    assertNotNull(statesDS);
    params.put("url", TestData.url(this, "places.shp"));
    placesDS = DataStoreFinder.getDataStore(params);
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
  public void testSimple() throws InterruptedException, ExecutionException, CQLException, IOException {
    Name name = new NameImpl("TableJoin", "joinTables");
    org.geotools.process.Process process = Processors.createProcess(name);

    ProcessExecutor engine = Processors.newProcessExecutor(2);

    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];
    String iname = stateIncDS.getTypeNames()[0];
    String typeName = statesDS.getSchema(sname).getTypeName();
    Join j = new Join(typeName, ECQL.toFilter("\"STATE_NAME\" = \"State\""));
    QueryType query = Wfs20Factory.eINSTANCE.createQueryType();
    query.getTypeNames().add(sname);
    query.getTypeNames().add(iname);
    query.setFilter(j.getJoinFilter());

    WFSConfiguration configuration = new org.geotools.wfs.v2_0.WFSConfiguration();
    org.geotools.xml.Encoder encoder = new org.geotools.xml.Encoder(configuration);

    // create an output stream
    ByteArrayOutputStream xml = new ByteArrayOutputStream();

    // encode
    encoder.encode(query, org.geotools.wfs.v2_0.WFS.Query, xml);

    String jxml = xml.toString("UTF-8");
    xml.close();
    System.out.println(jxml);
    Map<String, Object> input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateIncDS.getFeatureSource(iname).getFeatures(), "join", jxml);

    Progress working = engine.submit(process, input);

    Map<String, Object> result = working.get(); // get is BLOCKING
    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");

    assertEquals("Wrong number of states!", 49, out.size());

    String state_name = "STATE_NAME";

    String inc80 = "inc1980";

    FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    SortBy[] sortBy = new SortBy[1];

    sortBy[0] = ff.sort(inc80, SortOrder.DESCENDING);

    SortedSimpleFeatureCollection sort = new SortedSimpleFeatureCollection(out, sortBy);

    assertNotNull(sort);
    assertEquals("Wrong number of states!", 49, sort.size());
    SimpleFeature first = DataUtilities.first(sort);
    assertNotNull("Sort failed", first);
    assertEquals("District of Columbia", first.getAttribute(state_name));
    assertEquals(12251, ((Integer) first.getAttribute("inc1980")).intValue());

    sortBy[0] = ff.sort(inc80, SortOrder.ASCENDING);

    sort = new SortedSimpleFeatureCollection(out, sortBy);
    SimpleFeature last = DataUtilities.first(sort);
    assertEquals("Mississippi", last.getAttribute(state_name));
    assertEquals(6573, ((Integer) last.getAttribute(inc80)).intValue());
  }
  

  @Test
  public void testAlias() throws InterruptedException, ExecutionException, CQLException, IOException {
    Name name = new NameImpl("TableJoin", "joinTables");
    org.geotools.process.Process process = Processors.createProcess(name);

    ProcessExecutor engine = Processors.newProcessExecutor(2);

    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];
    String iname = stateUnempDS.getTypeNames()[0];
    String typeName = statesDS.getSchema(sname).getTypeName();
    Join j = new Join(typeName, ECQL.toFilter("strToLowerCase(\"A.STATE_NAME\") = strToLowerCase(\"STATE_NAME\")"));
    j.setAlias("A");
    QueryType query = Wfs20Factory.eINSTANCE.createQueryType();
    query.getTypeNames().add(sname);

    query.setFilter(j.getJoinFilter());
    query.getAliases().add("A");

    WFSConfiguration configuration = new org.geotools.wfs.v2_0.WFSConfiguration();
    org.geotools.xml.Encoder encoder = new org.geotools.xml.Encoder(configuration);

    // create an output stream
    ByteArrayOutputStream xml = new ByteArrayOutputStream();

    // encode
    encoder.encode(query, org.geotools.wfs.v2_0.WFS.Query, xml);

    String jxml = xml.toString("UTF-8");
    xml.close();
    System.out.println(jxml);
    Map<String, Object> input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateUnempDS.getFeatureSource(iname).getFeatures(), "join", jxml);

    Progress working = engine.submit(process, input);

    Map<String, Object> result = working.get(); // get is BLOCKING
    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");

    assertEquals("Wrong number of states!", 49, out.size());

    String state_name = "STATE_NAME";

    String valueAtt = "unemp";

    FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    SortBy[] sortBy = new SortBy[1];

    sortBy[0] = ff.sort(valueAtt, SortOrder.DESCENDING);

    SortedSimpleFeatureCollection sort = new SortedSimpleFeatureCollection(out, sortBy);
    /*
     * try (SimpleFeatureIterator itr = sort.features()) { while (itr.hasNext())
     * { SimpleFeature f = itr.next(); System.out.println(String.format("%-20s",
     * f.getAttribute(1).toString()) + "\t" + f.getAttribute(valueAtt) ); } }
     */
    assertNotNull(sort);
    assertEquals("Wrong number of states!", 49, sort.size());
    SimpleFeature first = DataUtilities.first(sort);
    assertNotNull("Sort failed", first);
    assertEquals("ILLINOIS", first.getAttribute(state_name));
    assertEquals(6.5, ((Double) first.getAttribute(valueAtt)).doubleValue(), 1e-6);

    sortBy[0] = ff.sort(valueAtt, SortOrder.ASCENDING);

    sort = new SortedSimpleFeatureCollection(out, sortBy);
    SimpleFeature last = DataUtilities.first(sort);
    assertEquals("SOUTH DAKOTA", last.getAttribute(state_name));
    assertEquals(2.5, ((Double) last.getAttribute(valueAtt)).doubleValue(), 1e-6);
  }

  @Test
  public void testBadInputs() throws IOException, CQLException {
    Name name = new NameImpl("TableJoin", "joinTables");
    org.geotools.process.Process process = Processors.createProcess(name);

    ProcessExecutor engine = Processors.newProcessExecutor(2);

    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];
    String iname = stateIncDS.getTypeNames()[0];
    Join j = new Join(statesDS.getSchema(sname).getTypeName(), ECQL.toFilter("\"STATE_NAME\" = \"State\""));
    Map<String, Object> input = new KVP("target",
        /* statesDS.getFeatureSource(sname).getFeatures() */ null, "source",
        stateIncDS.getFeatureSource(iname).getFeatures(), "join", j);

    Progress working = engine.submit(process, input);

    try {
      try {
        @SuppressWarnings("unused")
        Map<String, Object> result = working.get();
        fail("Shouldn't be able to use null inputs");
      } catch (ProcessException pe) {
        // this is expected!
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } // get is BLOCKING

    input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        /* stateIncDS.getFeatureSource(iname).getFeatures() */ null, "join", j);

    working = engine.submit(process, input);

    try {
      try {
        @SuppressWarnings("unused")
        Map<String, Object> result = working.get();
        fail("Shouldn't be able to use null inputs");
      } catch (ProcessException pe) {
        // this is expected!
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } // get is BLOCKING

    input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateIncDS.getFeatureSource(iname).getFeatures(), "join", /* j */ null);

    working = engine.submit(process, input);

    try {
      try {
        @SuppressWarnings("unused")
        Map<String, Object> result = working.get();
        fail("Shouldn't be able to use null inputs");
      } catch (ProcessException pe) {
        // this is expected!
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } // get is BLOCKING

    j = new Join(statesDS.getSchema(sname).getTypeName(), null);

    input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateIncDS.getFeatureSource(iname).getFeatures(), "join", j);

    working = engine.submit(process, input);

    try {
      try {
        @SuppressWarnings("unused")
        Map<String, Object> result = working.get();
        fail("Shouldn't be able to use null inputs");
      } catch (ProcessException pe) {
        // this is expected!
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } // get is BLOCKING
  }

  @Test
  public void testDesc() {

    Name name = new NameImpl("TableJoin", "joinTables");

    Map<String, Parameter<?>> paramInfo = Processors.getParameterInfo(name);
    assertNotNull(paramInfo);
    for (Entry<String, Parameter<?>> entry : paramInfo.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue().getName() + " " + entry.getValue().getDescription());
    }
  }

  @Test
  public void testSimpleJoinTable() throws IOException {
    Name name = new NameImpl("TableJoin", "simpleJoinTables");
    org.geotools.process.Process process = Processors.createProcess(name);

    ProcessExecutor engine = Processors.newProcessExecutor(2);

    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];
    String iname = stateIncDS.getTypeNames()[0];
    String typeName = statesDS.getSchema(sname).getTypeName();
    String cql = "\"STATE_NAME\" = \"State\"";

    KVP input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateIncDS.getFeatureSource(iname).getFeatures(), "joinfilter", cql);

    Progress working = engine.submit(process, input);

    try {
      try {
        @SuppressWarnings("unused")
        Map<String, Object> result = working.get();
        SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");

        assertEquals("Wrong number of states!", 49, out.size());

        String state_name = "STATE_NAME";

        String inc80 = "inc1980";

        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        SortBy[] sortBy = new SortBy[1];

        sortBy[0] = ff.sort(inc80, SortOrder.DESCENDING);

        SortedSimpleFeatureCollection sort = new SortedSimpleFeatureCollection(out, sortBy);

        assertNotNull(sort);
        assertEquals("Wrong number of states!", 49, sort.size());
        SimpleFeature first = DataUtilities.first(sort);
        assertNotNull("Sort failed", first);
        assertEquals("District of Columbia", first.getAttribute(state_name));
        assertEquals(12251, ((Integer) first.getAttribute("inc1980")).intValue());

        sortBy[0] = ff.sort(inc80, SortOrder.ASCENDING);

        sort = new SortedSimpleFeatureCollection(out, sortBy);
        SimpleFeature last = DataUtilities.first(sort);
        assertEquals("Mississippi", last.getAttribute(state_name));
        assertEquals(6573, ((Integer) last.getAttribute(inc80)).intValue());
      } catch (ProcessException pe) {
        fail();
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      fail();
    } // get is BLOCKING

  }

  @Test
  public void testSimpleJoinAlias() throws InterruptedException, ExecutionException, CQLException, IOException {
    Name name = new NameImpl("TableJoin", "simpleJoinTables");
    org.geotools.process.Process process = Processors.createProcess(name);

    ProcessExecutor engine = Processors.newProcessExecutor(2);

    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];
    String iname = stateUnempDS.getTypeNames()[0];
    String typeName = statesDS.getSchema(sname).getTypeName();
    String cql = "strToLowerCase(\"A.STATE_NAME\") = strToLowerCase(\"STATE_NAME\")";
    String alias = "A";

    Map<String, Object> input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateUnempDS.getFeatureSource(iname).getFeatures(), "joinfilter", cql, "alias", alias);

    Progress working = engine.submit(process, input);

    Map<String, Object> result = working.get(); // get is BLOCKING
    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");

    assertEquals("Wrong number of states!", 49, out.size());

    String state_name = "STATE_NAME";

    String valueAtt = "unemp";

    FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    SortBy[] sortBy = new SortBy[1];

    sortBy[0] = ff.sort(valueAtt, SortOrder.DESCENDING);

    SortedSimpleFeatureCollection sort = new SortedSimpleFeatureCollection(out, sortBy);
    /*
     * try (SimpleFeatureIterator itr = sort.features()) { while (itr.hasNext())
     * { SimpleFeature f = itr.next(); System.out.println(String.format("%-20s",
     * f.getAttribute(1).toString()) + "\t" + f.getAttribute(valueAtt) ); } }
     */
    assertNotNull(sort);
    assertEquals("Wrong number of states!", 49, sort.size());
    SimpleFeature first = DataUtilities.first(sort);
    assertNotNull("Sort failed", first);
    assertEquals("ILLINOIS", first.getAttribute(state_name));
    assertEquals(6.5, ((Double) first.getAttribute(valueAtt)).doubleValue(), 1e-6);

    sortBy[0] = ff.sort(valueAtt, SortOrder.ASCENDING);

    sort = new SortedSimpleFeatureCollection(out, sortBy);
    SimpleFeature last = DataUtilities.first(sort);
    assertEquals("SOUTH DAKOTA", last.getAttribute(state_name));
    assertEquals(2.5, ((Double) last.getAttribute(valueAtt)).doubleValue(), 1e-6);
  }

  @Test
  public void testSpatialJoinAlias() throws InterruptedException, ExecutionException, CQLException, IOException {
    Name name = new NameImpl("TableJoin", "simpleJoinTables");
    org.geotools.process.Process process = Processors.createProcess(name);

    ProcessExecutor engine = Processors.newProcessExecutor(2);

    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];

    String pname = placesDS.getTypeNames()[0];

    String cql = "contains(\"the_geom\",\"A.the_geom\")";
    String alias = "A";

    Map<String, Object> input = new KVP("source", statesDS.getFeatureSource(sname).getFeatures(), "target",
        placesDS.getFeatureSource(pname).getFeatures(), "joinfilter", cql, "alias", alias);

    Progress working = engine.submit(process, input);

    Map<String, Object> result = working.get(); // get is BLOCKING
    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");



    /*try (SimpleFeatureIterator itr = out.features()) {
      while (itr.hasNext()) {
        SimpleFeature f = itr.next();
        System.out.println(
            String.format("%-20s\t%s", f.getAttribute("NAME").toString(), f.getAttribute("STATE_NAME").toString()));
      }
    }*/

    Filter f = ECQL.toFilter("\"NAME\"='Danville'");

    SimpleFeatureCollection sub = out.subCollection(f);
    assertTrue(sub.size() > 0);
    SimpleFeature danville = DataUtilities.first(sub);
    assertEquals("Virginia",danville.getAttribute("STATE_NAME"));

  }

  @Test
  public void testSimpleJoinPropertiesAlias() throws InterruptedException, ExecutionException, CQLException, IOException {
    Name name = new NameImpl("TableJoin", "simpleJoinTables");
    org.geotools.process.Process process = Processors.createProcess(name);
  
    ProcessExecutor engine = Processors.newProcessExecutor(2);
  
    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];
    String iname = stateUnempDS.getTypeNames()[0];
    String typeName = statesDS.getSchema(sname).getTypeName();
    String cql = "strToLowerCase(\"A.STATE_NAME\") = strToLowerCase(\"STATE_NAME\")";
    String alias = "A";
  
    Map<String, Object> input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateUnempDS.getFeatureSource(iname).getFeatures(), "joinfilter", cql, "alias", alias,
        "propertyNames","STATE_NAME,Rank,unemp");
  
    Progress working = engine.submit(process, input);
  
    Map<String, Object> result = working.get(); // get is BLOCKING
    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");
  
    SimpleFeatureType outSchema = out.getSchema();
    //System.out.println(outSchema);
    //
    assertEquals(3, outSchema.getAttributeCount());
    assertEquals("Wrong number of states!", 49, out.size());
  
    String state_name = "STATE_NAME";
  
    String valueAtt = "unemp";
  
    FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    SortBy[] sortBy = new SortBy[1];
  
    sortBy[0] = ff.sort(valueAtt, SortOrder.DESCENDING);
  
    SortedSimpleFeatureCollection sort = new SortedSimpleFeatureCollection(out, sortBy);
    
   /* try (SimpleFeatureIterator itr = sort.features()) {
      while (itr.hasNext()) {
        SimpleFeature f = itr.next();
        System.out.println(String.format("%-20s", f.getAttribute(state_name).toString()) + "\t" + f.getAttribute(valueAtt)+"\t" + f.getAttribute("Rank"));
      }
    }*/

    assertNotNull(sort);
    assertEquals("Wrong number of states!", 49, sort.size());
    SimpleFeature first = DataUtilities.first(sort);
    assertNotNull("Sort failed", first);
    assertEquals("Illinois", first.getAttribute(state_name));
    assertEquals(6.5, ((Double) first.getAttribute(valueAtt)).doubleValue(), 1e-6);
  
    sortBy[0] = ff.sort(valueAtt, SortOrder.ASCENDING);
  
    sort = new SortedSimpleFeatureCollection(out, sortBy);
    SimpleFeature last = DataUtilities.first(sort);
    assertEquals("South Dakota", last.getAttribute(state_name));
    assertEquals(2.5, ((Double) last.getAttribute(valueAtt)).doubleValue(), 1e-6);
  }
  
  @Test
  public void testSimpleFilteredJoinTable() throws IOException {
    Name name = new NameImpl("TableJoin", "simpleJoinTables");
    org.geotools.process.Process process = Processors.createProcess(name);

    ProcessExecutor engine = Processors.newProcessExecutor(2);

    // quick map of inputs
    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];
    String iname = stateIncDS.getTypeNames()[0];
    String typeName = statesDS.getSchema(sname).getTypeName();
    String cql = "\"STATE_NAME\" = \"State\"";
    String outCQL = "STATE_ABBR like 'N%'";
    KVP input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateIncDS.getFeatureSource(iname).getFeatures(), "joinfilter", cql, "outputfilter", outCQL);

    Progress working = engine.submit(process, input);

    try {
      try {
        @SuppressWarnings("unused")
        Map<String, Object> result = working.get();
        SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");

        assertEquals("Wrong number of states!", 8, out.size());

        String state_name = "STATE_NAME";

        String inc80 = "inc1980";

        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        SortBy[] sortBy = new SortBy[1];

        sortBy[0] = ff.sort(inc80, SortOrder.DESCENDING);

        SortedSimpleFeatureCollection sort = new SortedSimpleFeatureCollection(out, sortBy);
        /*try (SimpleFeatureIterator itr = sort.features()) {
          while (itr.hasNext()) {
            SimpleFeature f = itr.next();
            System.out.println(String.format("%-20s", f.getAttribute(state_name).toString()) + "\t"
                + f.getAttribute(inc80) );
          }
        }*/
        assertNotNull(sort);
        assertEquals("Wrong number of states!", 8, sort.size());
        SimpleFeature first = DataUtilities.first(sort);
        assertNotNull("Sort failed", first);
        assertEquals("New Jersey", first.getAttribute(state_name));
        assertEquals(10966, ((Integer) first.getAttribute("inc1980")).intValue());

        sortBy[0] = ff.sort(inc80, SortOrder.ASCENDING);

        sort = new SortedSimpleFeatureCollection(out, sortBy);
        SimpleFeature last = DataUtilities.first(sort);
        assertEquals("North Carolina", last.getAttribute(state_name));
        assertEquals(7780, ((Integer) last.getAttribute(inc80)).intValue());
      } catch (ProcessException pe) {
        fail();
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      fail();
    } // get is BLOCKING

  }
  
  @Test
  public void testSimpleJoinPropertiesAliasFiltered() throws InterruptedException, ExecutionException, CQLException, IOException {
    Name name = new NameImpl("TableJoin", "simpleJoinTables");
    org.geotools.process.Process process = Processors.createProcess(name);
  
    ProcessExecutor engine = Processors.newProcessExecutor(2);
  
    // quick map of inputs
    String sname = statesDS.getTypeNames()[0];
    String iname = stateUnempDS.getTypeNames()[0];
    String typeName = statesDS.getSchema(sname).getTypeName();
    String cql = "strToLowerCase(\"A.STATE_NAME\") = strToLowerCase(\"STATE_NAME\")";
    String alias = "A";
    String outCQL = "STATE_ABBR like 'N%'";
    Map<String, Object> input = new KVP("target", statesDS.getFeatureSource(sname).getFeatures(), "source",
        stateUnempDS.getFeatureSource(iname).getFeatures(), "joinfilter", cql, "alias", alias,
        "propertyNames","STATE_NAME,Rank,unemp", "outputfilter",outCQL);
  
    Progress working = engine.submit(process, input);
  
    Map<String, Object> result = working.get(); // get is BLOCKING
    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");
  
    SimpleFeatureType outSchema = out.getSchema();
    //System.out.println(outSchema);
    //
    assertEquals(3, outSchema.getAttributeCount());
    assertEquals("Wrong number of states!", 8, out.size());
  
    String state_name = "STATE_NAME";
  
    String valueAtt = "unemp";
  
    FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    SortBy[] sortBy = new SortBy[1];
  
    sortBy[0] = ff.sort(valueAtt, SortOrder.DESCENDING);
  
    SortedSimpleFeatureCollection sort = new SortedSimpleFeatureCollection(out, sortBy);
    
    /*try (SimpleFeatureIterator itr = sort.features()) {
      while (itr.hasNext()) {
        SimpleFeature f = itr.next();
        System.out.println(String.format("%-20s", f.getAttribute(state_name).toString()) + "\t" + f.getAttribute(valueAtt)+"\t" + f.getAttribute("Rank"));
      }
    }*/

    assertNotNull(sort);
    assertEquals("Wrong number of states!", 8, sort.size());
    SimpleFeature first = DataUtilities.first(sort);
    assertNotNull("Sort failed", first);
    assertEquals("New Mexico", first.getAttribute(state_name));
    assertEquals(6.2, ((Double) first.getAttribute(valueAtt)).doubleValue(), 1e-6);
  
    sortBy[0] = ff.sort(valueAtt, SortOrder.ASCENDING);
  
    sort = new SortedSimpleFeatureCollection(out, sortBy);
    SimpleFeature last = DataUtilities.first(sort);
    assertEquals("New Hampshire", last.getAttribute(state_name));
    assertEquals(2.6, ((Double) last.getAttribute(valueAtt)).doubleValue(), 1e-6);
  }
}

package com.astuntechnology.wps.picker;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.NameImpl;
import org.geotools.filter.FilterTransformer;
import org.geotools.process.ProcessExecutor;
import org.geotools.process.Processors;
import org.geotools.process.Progress;
import org.geotools.util.KVP;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.type.Name;
import org.opengis.filter.FilterFactory2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class PickAndUnionTest {
	private DataStore statesDS;
	FilterTransformer transform = new FilterTransformer();
	private static GeometryFactory gf = new GeometryFactory();


	private static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
	@Before
	public void setup() throws IOException {
		Map<String, Object> params = new HashMap<>();
		params.put("url", org.geotools.TestData.url("shapes/statepop.shp"));
		statesDS = DataStoreFinder.getDataStore(params);
		assertNotNull(statesDS);
		transform.setIndentation(2);
	}
	@Test
	public void testSimpleCQL() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");
		
		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(),
				"filter","STATE_NAME = 'Pennsylvania'");
		Progress working = engine.submit(process, input);

	    Map<String, Object> result = working.get(); // get is BLOCKING
	    Geometry out = (Geometry) result.get("result");
	    assertNotNull(out);
	    assertFalse(out.contains(gf.createPoint(new Coordinate(-99,38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24,41.27))));
	}
	
	@Test
	public void testDeselect() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");
		
		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(),
				"filter","STATE_ABBR in ('PA','OH')");
		Progress working = engine.submit(process, input);

	    Map<String, Object> result = working.get(); // get is BLOCKING
	    Geometry out = (Geometry) result.get("result");
	    assertNotNull(out);
	    assertFalse(out.contains(gf.createPoint(new Coordinate(-99,38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24,41.27))));
		
		//now deselect PA
		Map<String, Object> input2 = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(),
				"filter","STATE_ABBR in ('PA')","geometry",out,"subtract",Boolean.TRUE);
		working = engine.submit(process, input2);

	    result = working.get(); // get is BLOCKING
	    out = (Geometry) result.get("result");
	    
	    assertNotNull(out);
	    assertFalse(out.contains(gf.createPoint(new Coordinate(-99,38))));
		assertFalse(out.contains(gf.createPoint(new Coordinate(-78.24,41.27))));
		
	}
	
	@Test
	public void testSelect() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");
		
		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(),
				"filter","STATE_ABBR in ('PA','OH')");
		Progress working = engine.submit(process, input);

	    Map<String, Object> result = working.get(); // get is BLOCKING
	    Geometry out = (Geometry) result.get("result");
	    assertNotNull(out);
	    assertFalse(out.contains(gf.createPoint(new Coordinate(-99,38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24,41.27))));
		
		//now select VA
		Map<String, Object> input2 = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(),
				"filter","STATE_ABBR in ('VA')","geometry",out);
		working = engine.submit(process, input2);

	    result = working.get(); // get is BLOCKING
	    out = (Geometry) result.get("result");
	    
	    assertNotNull(out);
	    assertFalse(out.contains(gf.createPoint(new Coordinate(-99,38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24,41.27))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.25,37.54))));
		
	}
}

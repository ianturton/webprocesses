package com.astuntechnology.wps.picker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.xml.transform.TransformerException;

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
import org.opengis.filter.PropertyIsEqualTo;

public class PickerProcessTest {

	private DataStore statesDS;
	FilterTransformer transform = new FilterTransformer();


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
	public void testGetFeaturesSimpleCQL() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("Picker", "getFeatures");
		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(),
				"filter","STATE_NAME = 'Pennsylvania'");
		Progress working = engine.submit(process, input);

	    Map<String, Object> result = working.get(); // get is BLOCKING
	    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");
	    assertEquals("Wrong number of states",1, out.size());
	    
	    
	}
	
	@Test
	public void testGetFeaturesComplexCQL() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("Picker", "getFeatures");
		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(),
				"filter","contains(the_geom,Point( -77.858611 40.791389))");
		Progress working = engine.submit(process, input);

	    Map<String, Object> result = working.get(); // get is BLOCKING
	    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");
	    assertEquals("Wrong number of states",1, out.size());
	    
	    
	}
	
	@Test
	public void testGetFeaturesSimpleOGC() throws IOException, InterruptedException, ExecutionException, TransformerException {
		Name name = new NameImpl("Picker", "getFeatures");
		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		PropertyIsEqualTo f = ff.equal(ff.property("STATE_NAME"), ff.literal("Pennsylvania"), true);
		String filterXML = transform.transform( f );
		
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(),
				"filter",filterXML );
		Progress working = engine.submit(process, input);

	    Map<String, Object> result = working.get(); // get is BLOCKING
	    SimpleFeatureCollection out = (SimpleFeatureCollection) result.get("result");
	    assertEquals("Wrong number of states",1, out.size());
	    
	}

}

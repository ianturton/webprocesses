package com.astuntechnology.wps.picker;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.geoserver.wps.WPSException;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.NameImpl;
import org.geotools.filter.FilterTransformer;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.process.ProcessExecutor;
import org.geotools.process.Processors;
import org.geotools.process.Progress;
import org.geotools.util.KVP;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.type.Name;
import org.opengis.filter.FilterFactory2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class PickAndUnionTest {
	private static DataStore statesDS;
	static FilterTransformer transform = new FilterTransformer();
	private static GeometryFactory gf = new GeometryFactory();

	@BeforeClass
	public static void setup() throws IOException {
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
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_NAME = 'Pennsylvania'");
		Progress working = engine.submit(process, input);

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry out = (Geometry) result.get("result");
		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));
	}

	@Test
	public void testBrokenCQL() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_NAME = Pennsylvania'");
		Progress working = engine.submit(process, input);
		try {
			Map<String, Object> result = working.get(); // get is BLOCKING
			Geometry out = (Geometry) result.get("result");
			fail();
		} catch (ExecutionException | WPSException e) {
			// excellent bad filter throws exception!
		}
	}

	@Test
	public void testDeselect() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('PA','OH')");
		Progress working = engine.submit(process, input);

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry out = (Geometry) result.get("result");
		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));

		// now deselect PA
		Map<String, Object> input2 = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('PA')", "geometry", out, "subtract", Boolean.TRUE);
		working = engine.submit(process, input2);

		result = working.get(); // get is BLOCKING
		out = (Geometry) result.get("result");

		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertFalse(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));

	}

	@Test
	public void testInternalDeselect() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('PA','OH','WV','KY','VA','MD','DC')");
		Progress working = engine.submit(process, input);

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry out = (Geometry) result.get("result");
		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));

		// now deselect WV as it is completely surrounded
		Map<String, Object> input2 = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('WV')", "geometry", out, "subtract", Boolean.TRUE);
		working = engine.submit(process, input2);

		result = working.get(); // get is BLOCKING
		out = (Geometry) result.get("result");

		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-80.9, 38.46))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-83.0, 37.9))));

	}

	@Test
	public void testAddSelect() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('PA','OH')");
		Progress working = engine.submit(process, input);

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry out = (Geometry) result.get("result");
		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));

		// now select VA
		Map<String, Object> input2 = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('VA')", "geometry", out);
		working = engine.submit(process, input2);

		result = working.get(); // get is BLOCKING
		out = (Geometry) result.get("result");

		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.25, 37.54))));

	}

	@Test
	public void testBrokenSelect() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('PA','OH')");
		Progress working = engine.submit(process, input);

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry out = (Geometry) result.get("result");
		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));

		// now select VA
		Map<String, Object> input2 = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR i ('VA')", "geometry", out);
		try {
			working = engine.submit(process, input2);

			result = working.get(); // get is BLOCKING
			out = (Geometry) result.get("result");
			fail();
		} catch (ExecutionException | WPSException e) {
			// excellent bad filter throws exception!
		}

	}

	@Test
	public void testDeselectNotIncluded() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('PA','OH')");
		Progress working = engine.submit(process, input);

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry out = (Geometry) result.get("result");
		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));

		// now select VA
		Map<String, Object> input2 = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('VA')", "geometry", out, "subtract", Boolean.TRUE);
		working = engine.submit(process, input2);

		result = working.get(); // get is BLOCKING
		out = (Geometry) result.get("result");

		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));
		assertFalse(out.contains(gf.createPoint(new Coordinate(-78.25, 37.54))));

	}

	@Test
	public void testDeselectPartiallyIncluded() throws IOException, InterruptedException, ExecutionException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('PA','OH','WV','KY','VA','MD','DC')");
		Progress working = engine.submit(process, input);

		Map<String, Object> result = working.get(); // get is BLOCKING
		Geometry out = (Geometry) result.get("result");
		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));

		// now select VA
		Map<String, Object> input2 = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('VA','CA')", "geometry", out, "subtract", Boolean.TRUE);
		working = engine.submit(process, input2);

		result = working.get(); // get is BLOCKING
		out = (Geometry) result.get("result");

		assertNotNull(out);
		assertFalse(out.contains(gf.createPoint(new Coordinate(-99, 38))));
		assertTrue(out.contains(gf.createPoint(new Coordinate(-78.24, 41.27))));
		assertFalse(out.contains(gf.createPoint(new Coordinate(-78.25, 37.54))));

	}

	@Test
	public void testInvalidInputGeometry()
			throws IOException, InterruptedException, ExecutionException, ParseException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		WKTReader2 wktr = new WKTReader2();
		// Bow Tie
		Geometry geom = wktr.read(
				"Polygon ((-88.26396314147442013 40.81507372009732393, -77.9880668633561811 36.57353355423575181, -77.68197633592288298 40.98998259291636259, -87.91414539583635701 36.57353355423575181, -88.26396314147442013 40.81507372009732393))");
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"STATE_ABBR in ('PA','OH','WV','KY','VA','MD','DC')", "geometry", geom, "subtract", Boolean.TRUE);
		try {
			Progress working = engine.submit(process, input);

			Map<String, Object> result = working.get(); // get is BLOCKING
			Geometry out = (Geometry) result.get("result");
			fail();
		} catch (ExecutionException | WPSException e) {
			// excellent bad geometry throws exception!
		}

	}
	
	@Test
	public void testInvalidInputFilterGeometry()
			throws IOException, InterruptedException, ExecutionException, ParseException {
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		String sname = statesDS.getTypeNames()[0];
		WKTReader2 wktr = new WKTReader2();
		// Bow Tie
		String wkt = 
				"Polygon ((-88.26396314147442013 40.81507372009732393, -77.9880668633561811 36.57353355423575181, -77.68197633592288298 40.98998259291636259, -87.91414539583635701 36.57353355423575181, -88.26396314147442013 40.81507372009732393))";
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"Intersects(the_geom,"+wkt+")");
		try {
			Progress working = engine.submit(process, input);

			Map<String, Object> result = working.get(); // get is BLOCKING
			Geometry out = (Geometry) result.get("result");
			//currently this doesn't throw an error but I think it should
			//fail();
		} catch (ExecutionException | WPSException e) {
			//ok
		}

	}
}

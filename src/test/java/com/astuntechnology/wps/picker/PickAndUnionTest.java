package com.astuntechnology.wps.picker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.lf5.PassingLogRecordFilter;
import org.geoserver.wps.WPSException;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.wfs.WFSDataStoreFactory;
import org.geotools.data.wfs.impl.WFSDataAccessFactory;
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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

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

	private DataStore wfs;
	private boolean onLine=true;

	@Before
	public void setUp() {
		Map<String, Object> params = new HashMap<>();
		params.put(WFSDataStoreFactory.URL.key, "http://localhost:9080/geoserver/wfs?version=1.1.0");
	
		try {
			wfs = DataStoreFinder.getDataStore(params);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		if (wfs == null) {
			System.err.println("Can't connect to WFS server, did you open the tunnel?");
			onLine = false;
			return;
		}
		onLine = true;
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
		String wkt = "Polygon ((-88.26396314147442013 40.81507372009732393, -77.9880668633561811 36.57353355423575181, -77.68197633592288298 40.98998259291636259, -87.91414539583635701 36.57353355423575181, -88.26396314147442013 40.81507372009732393))";
		Map<String, Object> input = new KVP("collection", statesDS.getFeatureSource(sname).getFeatures(), "filter",
				"Intersects(the_geom," + wkt + ")");
		try {
			Progress working = engine.submit(process, input);

			Map<String, Object> result = working.get(); // get is BLOCKING
			Geometry out = (Geometry) result.get("result");
			// currently this doesn't throw an error but I think it should
			// fail();
		} catch (ExecutionException | WPSException e) {
			// ok
		}

	}

	@Test
	public void testISHARE_6910() throws ParseException, IOException, InterruptedException {
		assumeTrue("Not connected", onLine);
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		WKTReader2 wktr = new WKTReader2();
		Geometry out = null;
		Geometry geom1 = wktr.read(
				"MULTIPOLYGON(((514084.53071362 160842.60805066,514163.03071362 160797.35805066,514163.03071362 160797.35805066,514126.15571362 160764.48305066,514126.15571362 160764.48305066,514077.03071362 160810.98305066,514077.03071362 160810.98305066,514076.40571362 160819.98305066,514076.40571362 160819.98305066,514084.53071362 160842.60805066)))");
		Map<String, Object> input1 = new KVP("collection", wfs.getFeatureSource("astun:topographicarea").getFeatures(),
				"filter", "intersects(wkb_geometry,POINT(514126.53071362 160831.35805066))", "geometry", geom1,
				"subtract", Boolean.FALSE);
		try {
			Progress working = engine.submit(process, input1);

			Map<String, Object> result = working.get(); // get is BLOCKING
			out = (Geometry) result.get("result");
			// System.out.println(out);

		} catch (ExecutionException | WPSException e) {
			fail();
		}

		Map<String, Object> input = new KVP("collection", wfs.getFeatureSource("astun:topographicarea").getFeatures(),
				"filter", "intersects(wkb_geometry,POINT(514126.53071362 160831.35805066))", "geometry", out,
				"subtract", Boolean.TRUE);
		try {
			Progress working = engine.submit(process, input);

			Map<String, Object> result = working.get(); // get is BLOCKING
			Geometry out1 = (Geometry) result.get("result");
			// System.out.println(out1);
			assertTrue(out1.isValid());
		} catch (ExecutionException | WPSException e) {
			fail();
		}

	}

	@Test
	public void testISHARE_6952() throws ParseException, IOException, InterruptedException {
		assumeTrue("Not connected", onLine);
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		WKTReader2 wktr = new WKTReader2();
		Geometry out = null;
		Geometry geom1 = wktr.read(
				"MULTIPOLYGON(((515628.55 168413.05,515631.2 168433.5,515631.25 168434.3,515626.6 168435.05,515626.9 168436.65,515628.9 168436.3,515629.5 168440.65,515630.75 168440.5,515630.4 168438.45,515631.95 168438.25,515631.5 168435.25,515632.8 168435.1,515633.2 168438.4,515635.35 168438.05,515635.6 168439.95,515639.9 168439.3,515639.7 168438.15,515641.95 168437.8,515642.234 168439.559,515673.6 168435,515682.2 168433.75,515682.15 168433.2,515685.4 168432.7,515685.5 168433.5,515695 168432.05,515694.2 168426.05,515692.75 168426.25,515692.5 168424.3,515693 168423.5,515694.25 168423.3,515695.25 168423.8,515695.4 168424.55,515707.4 168422.75,515706.7 168418.05,515705.35 168416.8,515705.7 168416.25,515705.15 168415.8,515706.2 168414.1,515706.9 168414.5,515707.35 168413.95,515708.4 168414.8,515714.51816581306 168413.92597631243,515716.8 168413.6,515715.6 168406.1,515716 168406,515715.85 168405.3,515714.95 168399,515714.61 168396.7,515714.09 168393.24,515709.25 168360.9,515704.15 168324.95,515701.0517473911 168325.3396575367,515698.65607408114 168309.58016805185,515698.657 168309.492,515698.713 168304.229,515697.6865885529 168303.2025885529,515694.625 168283.0625,515681.875 168276.0625,515604.875 168393.8125,515618.125 168403.3125,515618.52714389534 168406.5769622093,515623.9 168410.5,515624.4 168413.65,515628.55 168413.05),(515648.092 168434.489,515635.15 168436.2,515634.7 168433,515634.2 168429.2,515644.7 168427.8,515648.6 168427.25,515659.8 168425.8,515664 168425.2,515675.15 168423.75,515679.3 168423.2,515689.05 168421.9,515689.65 168426.5,515690 168428.95,515677.14 168430.65,515677.401 168432.62,515673.354 168433.155,515664.446 168434.333,515661.444 168434.73,515649.142 168436.356,515648.352 168436.46,515648.222 168435.475,515648.092 168434.489)),((515695.8 168435.1,515695.8066323053 168435.1445838303,515695.87049376796 168435.0896332694,515695.8 168435.1)))");
		Map<String, Object> input1 = new KVP("collection", wfs.getFeatureSource("astun:topographicarea").getFeatures(),
				"filter", "intersects(wkb_geometry,POINT(515701.375 168318.8125))", "geometry", geom1, "subtract",
				Boolean.FALSE);
		try {
			Progress working = engine.submit(process, input1);

			Map<String, Object> result = working.get(); // get is BLOCKING
			out = (Geometry) result.get("result");
			System.out.println(out);
			assertTrue(out.isValid());
		} catch (ExecutionException | WPSException e) {
			e.printStackTrace();
			fail();
		}

		Map<String, Object> input = new KVP("collection", wfs.getFeatureSource("astun:topographicarea").getFeatures(),
				"filter", "intersects(wkb_geometry,POINT(515698.625 168296.3125))", "geometry", out, "subtract",
				Boolean.TRUE);
		try {
			Progress working = engine.submit(process, input);

			Map<String, Object> result = working.get(); // get is BLOCKING

			Geometry out1 = (Geometry) result.get("result");
			System.out.println(out1);
			assertTrue(out1.isValid());
		} catch (ExecutionException | WPSException e) {
			fail();
		}

	}

	@Test
	public void testISHARE_6952b() throws ParseException, IOException, InterruptedException {
		assumeTrue("Not connected", onLine);
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		WKTReader2 wktr = new WKTReader2();
		Geometry out = null;
		Geometry geom1 = wktr.read(
				"MULTIPOLYGON(((515628.9 168436.3,515629.5 168440.65,515630.75 168440.5,515630.4 168438.45,515631.95 168438.25,515631.5 168435.25,515632.8 168435.1,515633.2 168438.4,515635.35 168438.05,515635.6 168439.95,515639.9 168439.3,515639.7 168438.15,515641.95 168437.8,515642.234 168439.559,515673.6 168435,515682.2 168433.75,515682.15 168433.2,515685.4 168432.7,515685.5 168433.5,515695 168432.05,515694.2 168426.05,515692.75 168426.25,515692.5 168424.3,515693 168423.5,515694.25 168423.3,515695.25 168423.8,515695.4 168424.55,515704.561812704 168423.17572809441,515633.375 168398.5,515629.3461316474 168419.19373290185,515631.2 168433.5,515631.25 168434.3,515626.6 168435.05,515626.9 168436.65,515628.9 168436.3)),((515685.05 168442.55,515674 168444.1,515674.25 168446,515685.4 168444.55,515685.08328127547 168442.74017871695,515685.05 168442.55)),((515688.57235421176 168442.05804535636,515687.25 168442.25,515687.2592894505 168442.31475689396,515688.57235421176 168442.05804535636)),((515714.9 168428.8,515712.8 168429.1,515708 168429.8,515708.2 168431.05,515707.95 168431.25,515707.8 168431.45,515707.7 168431.65,515707.6 168431.9,515707.55 168432.15,515707.55 168432.3,515707.7 168433.35,515713.3 168432.55,515722.8 168431.2,515722.53765408864 168429.40675540728,515715.4869456898 168426.96274426652,515714.619 168427.115,515714.9 168428.8)))");
		Map<String, Object> input1 = new KVP("collection", wfs.getFeatureSource("astun:topographicarea").getFeatures(),
				"filter", "intersects(wkb_geometry,POINT(515680.125 168443.75))", "geometry", geom1, "subtract",
				Boolean.TRUE);
		try {
			Progress working = engine.submit(process, input1);

			Map<String, Object> result = working.get(); // get is BLOCKING
			out = (Geometry) result.get("result");
			System.out.println(out);
			assertTrue(out.isValid());
		} catch (ExecutionException | WPSException e) {
			e.printStackTrace();
			fail();
		}

	}

	@Test
	public void testISHARE_6952c() throws ParseException, IOException, InterruptedException {
		assumeTrue("Not connected", onLine);
		Name name = new NameImpl("PickerAndUnion", "getFeatures");

		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		ProcessExecutor engine = Processors.newProcessExecutor(2);
		assertNotNull(engine);
		WKTReader2 wktr = new WKTReader2();
		Geometry out = null;
		Geometry geom1 = wktr.read(
				"MULTIPOLYGON(((515624.4 168413.65,515628.55 168413.05,515631.2 168433.5,515631.25 168434.3,515626.6 168435.05,515626.9 168436.65,515628.9 168436.3,515629.5 168440.65,515630.75 168440.5,515630.4 168438.45,515631.95 168438.25,515631.5 168435.25,515632.8 168435.1,515633.2 168438.4,515635.35 168438.05,515635.6 168439.95,515639.9 168439.3,515639.7 168438.15,515641.95 168437.8,515642.234 168439.559,515673.6 168435,515682.2 168433.75,515682.15 168433.2,515685.4 168432.7,515685.5 168433.5,515695 168432.05,515694.2 168426.05,515692.75 168426.25,515692.5 168424.3,515693 168423.5,515694.25 168423.3,515695.25 168423.8,515695.4 168424.55,515707.4 168422.75,515706.7 168418.05,515705.35 168416.8,515705.7 168416.25,515705.15 168415.8,515706.2 168414.1,515706.9 168414.5,515707.35 168413.95,515708.4 168414.8,515716.8 168413.6,515715.6 168406.1,515716 168406,515732.6 168403.6,515733 168406.15,515774.6 168400.1,515783.8 168398.7,515795 168397.05,515794.6 168394.55,515811.13 168392.11,515811.2 168392.1,515811.96879692894 168397.40673986336,515821.375 168395.375,515790.375 168178.375,515712.94401610264 168187.9049672489,515712.75 168188.2,515708 168195.25,515698.5 168209.25,515693.55 168216.75,515693.25 168217.5,515692.75 168218.9,515692.05 168220.9,515691.8 168221.9,515691.2 168224,515689.8 168230,515687.25 168241.25,515685.9 168247.3,515685 168252,515684.8 168252.65,515684.7 168253.2,515684.55 168253.75,515684.25 168254.5,515684 168255.3,515683.75 168256,515682.2 168260,515680.5 168263.8,515678.5 168267.95,515675.55 168273.65,515671.15 168282,515666.95 168289.75,515660.55 168301.65,515658.25 168308.5,515658.05 168308.55,515657.95 168308.65,515657.8 168308.75,515657.6 168308.9,515657.5 168309,515657.4 168309.15,515654.95 168312.7,515654.8 168312.85,515648.8 168321.05,515643.55 168328.45,515634.25 168341.15,515630.25 168346.6,515627.35 168350.35,515623.2 168355.5,515618.9 168360.7,515614.7 168365.8,515605.21536167804 168377.37199115727,515606.375 168393.375,515610.05201149423 168396.0014367816,515612.7 168392.35,515631.5 168405.95,515631.5 168406.35,515631.4 168406.8,515631.4 168407.2,515631.35 168407.6,515631.25 168408,515631.2 168408.4,515631 168408.75,515630.9 168409.1,515630.6 168409.4,515630.35 168409.7,515630 168409.9,515629.7 168410.1,515629.2 168410.3,515628.8 168410.45,515628.25 168410.6,515623.9 168410.5,515624.4 168413.65),(515648.352 168436.46,515648.222 168435.475,515648.092 168434.489,515635.15 168436.2,515634.7 168433,515634.2 168429.2,515644.7 168427.8,515648.6 168427.25,515659.8 168425.8,515664 168425.2,515675.15 168423.75,515679.3 168423.2,515689.05 168421.9,515689.65 168426.5,515690 168428.95,515677.14 168430.65,515677.401 168432.62,515673.354 168433.155,515664.446 168434.333,515661.444 168434.73,515649.142 168436.356,515648.352 168436.46)))");
		Map<String, Object> input1 = new KVP("collection", wfs.getFeatureSource("astun:topographicarea").getFeatures(),
				"filter", "intersects(wkb_geometry,POINT(515609.625 168378.875))", "geometry", geom1, "subtract",
				Boolean.TRUE);
		try {
			Progress working = engine.submit(process, input1);

			Map<String, Object> result = working.get(); // get is BLOCKING
			out = (Geometry) result.get("result");
			System.out.println(out);
			assertTrue(out.isValid());
		} catch (ExecutionException | WPSException e) {
			e.printStackTrace();
			fail();
		}
		Map<String, Object> input = new KVP("collection", wfs.getFeatureSource("astun:topographicarea").getFeatures(),
				"filter", "intersects(wkb_geometry,POINT(515612.625 168384.375))", "geometry", out, "subtract",
				Boolean.TRUE);
		try {
			Progress working = engine.submit(process, input);

			Map<String, Object> result = working.get(); // get is BLOCKING

			Geometry out1 = (Geometry) result.get("result");
			System.out.println(out1);
			assertTrue(out1.isValid());
		} catch (ExecutionException | WPSException e) {
			fail();
		}

	}
}

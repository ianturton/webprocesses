package com.astuntechnology.wps.picker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.NameImpl;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.process.Processors;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;

public class UnionProcessTest {
	private DataStore statesDS;
	
	private static GeometryFactory gf = new GeometryFactory();
	
	@Before
	public void setup() throws IOException {
		Map<String, Object> params = new HashMap<>();
		params.put("url", org.geotools.TestData.url("shapes/statepop.shp"));
		statesDS = DataStoreFinder.getDataStore(params);
		assertNotNull(statesDS);
		
	}
	@Test
	public void test() throws IOException, CQLException, SchemaException {
		SimpleFeatureSource fs = statesDS.getFeatureSource(statesDS.getTypeNames()[0]);
		SimpleFeatureCollection collection = fs.getFeatures();
		Name name = new NameImpl("Union", "getUnion");
		org.geotools.process.Process process = Processors.createProcess(name);
		assertNotNull("failed to get process", process);
		Geometry feature =  UnionProcess.getUnion(collection);
		assertNotNull(feature);
		assertTrue(feature instanceof MultiPolygon);
		
		Geometry geom = UnionProcess.getUnion(collection.subCollection(ECQL.toFilter("STATE_ABBR in ('PA','OH')")));
		assertNotNull(geom);
		
		assertFalse(geom .contains(gf.createPoint(new Coordinate(-99,38))));
		assertTrue(geom.contains(gf.createPoint(new Coordinate(-78.24,41.27))));
		
		
	}

}

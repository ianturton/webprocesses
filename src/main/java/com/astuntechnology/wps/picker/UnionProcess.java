package com.astuntechnology.wps.picker;

import java.util.logging.Logger;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.util.logging.Logging;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.util.InternationalString;

import org.locationtech.jts.geom.Geometry;

public class UnionProcess extends StaticMethodsProcessFactory {
	private static final Logger LOGGER = Logging.getLogger(com.astuntechnology.wps.picker.UnionProcess.class);

	public UnionProcess() {
		this(Text.text("Union"), "Union", UnionProcess.class);
	}

	public UnionProcess(InternationalString title, String namespace, Class targetClass) {
		super(title, namespace, targetClass);
	}

	@DescribeProcess(title = "Union", description = "A process that takes a feature collection and returns the union of thier geometries")
	@DescribeResult(description = "A feature containing the union of the geometries and an array of toids", type = SimpleFeatureCollection.class, primary = true)
	static public SimpleFeatureCollection getUnionCollection(
			@DescribeParameter(name = "collection", description = "The features containing the geometries to be unioned", min = 1) SimpleFeatureCollection collection)
			throws SchemaException {
		Geometry geom = null;
		StringBuilder toids = new StringBuilder();
		try (SimpleFeatureIterator features = collection.features()) {
			while (features.hasNext()) {
				SimpleFeature f = features.next();
				Geometry defaultGeometry = (Geometry) f.getDefaultGeometry();
				if (geom == null) {
					geom = defaultGeometry;
					toids.append(f.getID());
				} else {
					geom = geom.union(defaultGeometry);
					toids.append(',').append(f.getID());
				}
			}
		}
		SimpleFeatureType schema = DataUtilities.createType("union", "union:Geometry,toids:String");
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(schema);
		builder.set("union", geom);

		builder.set("toids", toids.toString());
		return DataUtilities.collection(builder.buildFeature(null));

	}

	@DescribeProcess(title = "Union", description = "A process that takes a feature collection and returns the union of thier geometries")
	@DescribeResult(description = "A feature containing the union of the geometries and an array of toids", type = Geometry.class, primary = true)
	static public Geometry getUnion(
			@DescribeParameter(name = "collection", description = "The features containing the geometries to be unioned", min = 1) SimpleFeatureCollection collection)
			throws SchemaException {
		Geometry geom = null;
		StringBuilder toids = new StringBuilder();
		try (SimpleFeatureIterator features = collection.features()) {
			while (features.hasNext()) {
				SimpleFeature f = features.next();
				Geometry defaultGeometry = (Geometry) f.getDefaultGeometry();
				if (geom == null) {
					geom = defaultGeometry;
					toids.append(f.getID());
				} else {
					geom = geom.union(defaultGeometry);
					toids.append(',').append(f.getID());
				}
			}
		}
		return geom;

	}

}

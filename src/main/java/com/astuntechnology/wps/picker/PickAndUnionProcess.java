package com.astuntechnology.wps.picker;

import java.util.logging.Logger;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.util.InternationalString;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

@SuppressWarnings("rawtypes")
public class PickAndUnionProcess extends StaticMethodsProcessFactory {
	private static final Logger LOGGER = Logging.getLogger(com.astuntechnology.wps.picker.PickAndUnionProcess.class);
	static private final GeometryFactory GF = new GeometryFactory();

	public PickAndUnionProcess() {
		this(Text.text("Picker"), "PickerAndUnion", PickAndUnionProcess.class);
	}

	@SuppressWarnings("unchecked")
	public PickAndUnionProcess(InternationalString title, String namespace, Class targetClass) {

		super(title, namespace, targetClass);

	}

	@DescribeProcess(title = "Pick and Union", description = "Return the union of the features that fulfil the provided filter and the geometry passed in.")
	@DescribeResult(name = "result", description = "The union of the selected features and the geometry", primary = true, type = Geometry.class)

	static public Geometry getFeatures(
			@DescribeParameter(name = "collection", description = "The features to be searched", min = 1) SimpleFeatureCollection collection,
			@DescribeParameter(name = "filter", description = "the filter to be used for the search (CQL or OGC)", min = 1) String filter,
			@DescribeParameter(name = "geometry", description = "Optional geometry to union with the results of the filter.", min = 0) Geometry geom,
			@DescribeParameter(name = "subtract", description = "If true then the matching geometries are removed from geometry, default false", min = 0) Boolean subtract) {
		boolean sub = false;
		if (subtract != null) {
			sub = subtract.booleanValue();
		}
		Geometry ret = GF.createPolygon((CoordinateSequence)null);
		Geometry union = null;
		SimpleFeatureCollection collection1 = PickerProcess.getFeatures(collection, filter);
		// now union the results
		try (SimpleFeatureIterator features = collection1.features()) {
			while (features.hasNext()) {
				SimpleFeature f = features.next();
				Geometry defaultGeometry = (Geometry) f.getDefaultGeometry();
				if (union == null) {
					union = defaultGeometry;

				} else {
					union = union.union(defaultGeometry);
				}
			}
		}
		if (union == null) {
			LOGGER.fine("Didn't find any new features to union");
			if(geom != null) {
				ret = geom;
			}
		} else if (geom != null) {

			// remove the matching geoms from the input geom if subtract is true
			if (sub) {
				LOGGER.fine("input geometry, returning difference of input and result of query");
				ret = geom.difference(union);
			} else { // else we union it
				LOGGER.fine("input geometry, returning union of input and result of query");
				ret = geom.union(union);
			}
		} else {
			LOGGER.fine("No input geometry, returning result of query");
			if (union != null) {
				ret = union;
			}
		}
		return ret;
	}
}

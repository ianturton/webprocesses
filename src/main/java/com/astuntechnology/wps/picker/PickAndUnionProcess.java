package com.astuntechnology.wps.picker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Function;
import org.opengis.util.InternationalString;

import com.astuntechnology.oraclefixer.DeSpiker;

@SuppressWarnings("rawtypes")
public class PickAndUnionProcess extends StaticMethodsProcessFactory {
	private static final Logger LOGGER = Logging.getLogger(com.astuntechnology.wps.picker.PickAndUnionProcess.class);
	static private final GeometryFactory GF = new GeometryFactory();
	static private final FilterFactory2 FF = CommonFactoryFinder.getFilterFactory2();
	static private final DeSpiker despiker = new DeSpiker();
	static private DataStore ds;

	public PickAndUnionProcess() {
		this(Text.text("Picker"), "PickerAndUnion", PickAndUnionProcess.class);
	}

	@SuppressWarnings("unchecked")
	public PickAndUnionProcess(InternationalString title, String namespace, Class targetClass) {

		super(title, namespace, targetClass);
		if (ds == null) {
			Map<String, Object> params = new HashMap<>();
			// TODO: read this in from a file
			params.put(PostgisNGDataStoreFactory.DBTYPE.key, PostgisNGDataStoreFactory.DBTYPE.sample);
			params.put(PostgisNGDataStoreFactory.DATABASE.key, "dataservices");
			params.put(PostgisNGDataStoreFactory.USER.key, "viewer");
			params.put(PostgisNGDataStoreFactory.PASSWD.key, "viewer");
			params.put(PostgisNGDataStoreFactory.PORT.key, 5436);
			params.put(PostgisNGDataStoreFactory.SCHEMA.key, "public");
			params.put(PostgisNGDataStoreFactory.HOST.key, "localhost");
			try {
				ds = DataStoreFinder.getDataStore(params);

			} catch (IOException e) {
				LOGGER.info("failed to open restrictions database. " + e.getMessage());

			}
		}
	}

	@DescribeProcess(title = "Pick and Union", description = "Return the union of the features that fulfil the provided filter and the geometry passed in.")
	@DescribeResult(name = "result", description = "The union of the selected features and the geometry", primary = true, type = Geometry.class)

	static public Geometry getFeatures(
			@DescribeParameter(name = "collection", description = "The features to be searched", min = 1) SimpleFeatureCollection collection,
			@DescribeParameter(name = "filter", description = "the filter to be used for the search (CQL or OGC)", min = 1) String filter,
			@DescribeParameter(name = "geometry", description = "Optional geometry to union with the results of the filter.", min = 0) Geometry geom,
			@DescribeParameter(name = "subtract", description = "If true then the matching geometries are removed from geometry, default false", min = 0) Boolean subtract,
			@DescribeParameter(name = "organisation", description = "The name of the organisation making the request, default none", min = 0) String organisation) {
		boolean sub = false;
		if (subtract != null) {
			sub = subtract.booleanValue();
		}
		Filter and = Filter.INCLUDE;
		Function xFunction = null;
		if (organisation != null && !organisation.isEmpty()) {

			try {
				SimpleFeatureSource source = ds.getFeatureSource("limited_to");

				Filter orgFilter = FF.and(FF.equal(FF.property("org"), FF.literal(organisation), false),
						FF.equal(FF.property("product"), FF.literal("ospremium"), false));
				SimpleFeatureCollection features = source.getFeatures(orgFilter);
				//System.out.println("got " + features.size() + " featuresi with " + orgFilter);
				String localGeomName = collection.getSchema().getGeometryDescriptor().getLocalName();
				if (features.size() < 10) {
					try (SimpleFeatureIterator itr = features.features()) {
						List<Filter> filters = new ArrayList<>();
						while (itr.hasNext()) {
							SimpleFeature feature = (SimpleFeature) itr.next();
							filters.add(FF.intersects(FF.property(localGeomName),
									FF.literal(feature.getDefaultGeometry())));
						}
						if (filters.isEmpty())
							and = null;
						else if (filters.size() > 1)
							and = FF.and(filters);
						else
							and = filters.get(0);
					}
				} else {
					// union the geoms together for the filter?
					try (SimpleFeatureIterator itr = features.features()) {
						List<Geometry> geoms = new ArrayList<>();
						while (itr.hasNext()) {
							SimpleFeature feature = (SimpleFeature) itr.next();
							geoms.add((Geometry) feature.getDefaultGeometry());
					}
						GeometryCollection geometryCollection = (GeometryCollection) GF.buildGeometry(geoms);

				        Geometry union = geometryCollection.union();
				        and = FF.intersects(FF.property(localGeomName), FF.literal(union));
					}
				}
			} catch (IOException e) { // TODO Auto-generated catch
				e.printStackTrace();
			}

			/*
			 * SimpleFeatureSource source; try { source = ds.getFeatureSource("limited_to");
			 * xFunction = FF.function("querySingle", FF.literal("astun:limited_to"),
			 * FF.property(source.getSchema().getGeometryDescriptor().getName()),
			 * FF.literal(ECQL.toCQL(orgFilter))); } catch (IOException e) { // TODO
			 * Auto-generated catch block e.printStackTrace(); }
			 */
		}
		Geometry ret = GF.createPolygon((CoordinateSequence) null);
		Geometry union = null;
		Filter fullFilter;
		if (and != null) {
			fullFilter = FF.and(PickerProcess.parseFilterString(filter), and);

		} else {
			fullFilter = PickerProcess.parseFilterString(filter);
		}
		//System.out.println(fullFilter);

		SimpleFeatureCollection collection1 = PickerProcess.getFeatures(collection, fullFilter);
		// now union the results
		try (SimpleFeatureIterator features = collection1.features()) {
			while (features.hasNext()) {
				SimpleFeature f = features.next();
				Geometry defaultGeometry = (Geometry) f.getDefaultGeometry();
				if (union == null) {
					if (!defaultGeometry.isEmpty())
						union = defaultGeometry;

				} else {
					if (!defaultGeometry.isEmpty())
						union = union.union(defaultGeometry);
				}
			}
		}
		if (union == null) {
			LOGGER.fine("Didn't find any new features to union");
			if (geom != null) {
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

		ret = despiker.despike(ret);

		return ret;
	}
}

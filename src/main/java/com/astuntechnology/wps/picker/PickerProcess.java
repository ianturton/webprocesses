package com.astuntechnology.wps.picker;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.geoserver.wps.WPSException;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.util.logging.Logging;
import org.geotools.xml.Configuration;
import org.geotools.xml.Parser;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.util.InternationalString;
import org.xml.sax.SAXException;

/**
 * Take a featureID or location and return the feature that matches it (or contains it)
 * 
 * @author ian
 *
 */
@SuppressWarnings("rawtypes")
public class PickerProcess extends StaticMethodsProcessFactory {
    private static final Logger LOGGER = Logging
            .getLogger(com.astuntechnology.wps.picker.PickerProcess.class);

    public PickerProcess() {
        this(Text.text("Picker"), "Picker", PickerProcess.class);
    }

    @SuppressWarnings("unchecked")
    public PickerProcess(InternationalString title, String namespace, Class<?> targetClass) {
        super(title, namespace, targetClass);
    }

    @DescribeProcess(title = "Picker", description = "Return the features that fulfil the provided filter")
    @DescribeResult(name = "result", description = "The matching feature(s)", primary = true, type = SimpleFeatureCollection.class)

    static public SimpleFeatureCollection getFeatures(
            @DescribeParameter(name = "collection", description = "The features to be searched", min = 1) SimpleFeatureCollection collection,
            @DescribeParameter(name = "filter", description = "the filter to be used for the search (CQL or OGC)", min = 1) String filter) {
        SimpleFeatureCollection ret = null;
        Filter f = parseFilterString(filter);
       // LOGGER.info("filter is " + f);
        if (f == null) {
            // failed to parse the filter
            ret = DataUtilities.collection(new ArrayList<SimpleFeature>());
        } else {

            ret = collection.subCollection(f);
            //System.out.println(DataUtilities.first(ret).getDefaultGeometry());
        }
        return ret;
    }

    /**
     * convert a CQL or OGC filter string into a filter
     * 
     * @param filter a string representation of the filter
     * @return a filter object or null if the string is unconvertible
     */
    private static Filter parseFilterString(String filter) {
        Filter ret = null;
        Exception cql = null, ogc = null;
        try {
            ret = ECQL.toFilter(filter);
            return ret;
        } catch (CQLException e) {
            // Do nothing but save the exception
            cql = e;
        }
        Configuration configuration = new org.geotools.filter.v1_0.OGCConfiguration();
        Parser parser = new Parser(configuration);

        try {
            ret = (Filter) parser.parse(new ByteArrayInputStream(filter.getBytes()));
            return ret;
        } catch (IOException | SAXException | ParserConfigurationException e) {
            ogc = e;
        }

        LOGGER.info("failed to convert " + filter + " to a filter.");
        StringBuilder builder = new StringBuilder();
        if (cql != null) {
            builder.append("CQL Error: " + cql.getLocalizedMessage());
        }
        if (ogc != null) {
            builder.append("OGC Filter Error: " + ogc.getLocalizedMessage());
        }
        String msg = builder.toString();
        throw new WPSException(msg);

    }
}

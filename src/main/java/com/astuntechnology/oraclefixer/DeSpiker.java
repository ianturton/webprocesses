package com.astuntechnology.oraclefixer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.util.logging.Logging;

import org.locationtech.jts.algorithm.Angle;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Triangle;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

public class DeSpiker {
	private static final Logger LOGGER = Logging.getLogger(DeSpiker.class.getName());
	private GeometryFactory GF = new GeometryFactory();
	private double angle_threshold = 0.005;
	private double point_distance = 0.01;
	private double area_threshold = 0.02;
	private double area_null = 0;

	public static void main(String[] args)
			throws ParseException, FileNotFoundException, org.locationtech.jts.io.ParseException {
		Polygon p = null;
		Options options = new Options();
		options.addOption("w", "wkt", true, "use the polygon in WKT (@filename or WKT as arg)");
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption('w')) {
			String wktFile = cmd.getOptionValue('w');
			WKTReader reader = new WKTReader();

			if (wktFile.startsWith("@")) {
				FileReader fileReader = new FileReader(new File(wktFile.substring(1)));
				p = (Polygon) reader.read(fileReader);
			} else {
				p = (Polygon) reader.read(wktFile);
			}

		}
		DeSpiker spiker = new DeSpiker();
		Polygon px = spiker.deDuplicate(p);
		if(px!=null&& !px.isEmpty()) {
			System.out.println("removed " + (p.getNumPoints() - px.getNumPoints()) + " duplicate points");
		}else {
			System.out.println("deleted "+p);
		}
		Polygon np = spiker.despike(p);
		WKTWriter writer = new WKTWriter();
		System.out.println(writer.write(np));
		System.out.println(p.getArea() + " to " + np.getArea());
		System.exit(0);

	}
	double tolerance = 0.005;
	public Geometry deDuplicate(Geometry in) {
		if (in == null || in.isEmpty() || in instanceof Point|| in instanceof MultiPoint) {
			return in;
		}
		if (in instanceof Polygon) {
			return deDuplicate((Polygon) in);
		}
		if (in instanceof LineString) {
			return deDuplicate((LineString) in);
		}
		if(in instanceof MultiPolygon || in instanceof MultiLineString || in instanceof GeometryCollection) {
			int n = in.getNumGeometries();
			ArrayList<Geometry> geomList = new ArrayList<>();
			for(int i=0;i<n;i++) {
				Geometry g = in.getGeometryN(i);
				geomList.add(deDuplicate(g));
			}
			return GF.buildGeometry(geomList);
		}
		System.err.println("unexpected geometry "+in);
		return null;
	}

	public Polygon deDuplicate(Polygon in) {
		if (in == null) {
			Polygon out = GF.createPolygon(null, null);
			return out;
		}
		LineString exteriorRing = in.getExteriorRing();

		LinearRing shell;
		LinearRing[] rings = new LinearRing[in.getNumInteriorRing()];

		shell = GF.createLinearRing(deDuplicate(exteriorRing).getCoordinateSequence());
		if (shell.isEmpty()) {
			return GF.createPolygon(null, null);
		}
		for (int i = 0; i < rings.length; i++) {

			rings[i] = GF.createLinearRing(deDuplicate(in.getInteriorRingN(i)).getCoordinateSequence());
		}

		Polygon out = GF.createPolygon(shell, rings);
		return out;

	}

	private LineString deDuplicate(LineString exteriorRing) {
		ArrayList<Coordinate> outRing = new ArrayList<>();
		int nPoints = exteriorRing.getNumPoints();
		Point lastPoint = exteriorRing.getPointN(0);
		for (int i = 1; i < nPoints; i++) {
			Point p = exteriorRing.getPointN(i);
			if (!p.isWithinDistance(lastPoint, tolerance)) {

				outRing.add(p.getCoordinate());
				lastPoint = p;
			}
		}
		
		
		if(outRing.size()>=2 ) {
			if (outRing.get(0) != outRing.get(outRing.size() - 1)) {
				outRing.add(outRing.get(0));
			}
			LOGGER.fine(outRing.toString());
			return GF.createLineString(outRing.toArray(new Coordinate[] {}));
		}else {
			return GF.createLineString((Coordinate[])null);
		}
	}

	/**
	 * DeSpiking algorithm based on
	 * http://gasparesganga.com/labs/postgis-normalize-geometry/
	 * 
	 * @param in
	 *            - a Polygon (may be null)
	 * @return a valid polygon (may be empty)
	 */
	public Polygon despike(Polygon in) {
		if (in == null) {
			Polygon out = GF.createPolygon(null, null);
			return out;
		}

		LineString exteriorRing = in.getExteriorRing();
		ArrayList<Coordinate> outRing = cleanRing(exteriorRing);
		LinearRing shell;
		List<LinearRing> rings = new ArrayList<>();
		if (outRing.isEmpty() || outRing.size()<2) {
			shell = null;
			return null;
			
		} else {
			shell = GF.createLinearRing(outRing.toArray(new Coordinate[] {}));

			for (int i = 0; i < in.getNumInteriorRing(); i++) {
				ArrayList<Coordinate> ring = cleanRing((LinearRing) in.getInteriorRingN(i));
				LinearRing lRing = GF.createLinearRing(ring.toArray(new Coordinate[] {}));
				
				if(!lRing.isEmpty())
					rings.add(lRing);
			}
		}
		Polygon out = GF.createPolygon(shell, rings.toArray(new LinearRing[] {}));
		return out;

	}

	/**
	 * Considering 3 adjacent points Pn-1, Pn and Pn+1, the point Pn will be
	 * removed in one of these cases:
	 * 
	 * Case 1 - Removing spikes Both the following conditions must be met:
	 * 
	 * The area obtained connecting those points (ie. the area of the triangle
	 * formed by Pn-1, Pn and Pn+1 points) is equal or smaller than
	 * PAR_area_threshold. The angle in Pn is equal or smaller than
	 * PAR_angle_threshold OR the distance between Pn-1 and Pn is equal or
	 * smaller than PAR_point_distance_threshold and the angle in Pn+1 is equal
	 * or smaller than PAR_angle_threshold OR the distance between Pn and Pn+1
	 * is equal or smaller than PAR_point_distance_threshold and the angle in
	 * Pn-1 is equal or smaller than PAR_angle_threshold.
	 * 
	 * @param ring
	 *            - a LineString with possible spikes
	 * @return - a List of Coordinates
	 */
	private ArrayList<Coordinate> cleanRing(LineString exteriorRing) {
		return cleanRing(exteriorRing, true);
	}
	private ArrayList<Coordinate> cleanRing(LineString exteriorRing, boolean close) {
		int nPoints = exteriorRing.getNumPoints();
		ArrayList<Coordinate> outRing = new ArrayList<>();
		for (int i = 1; i <= nPoints; i++) {
			boolean remove = false;
			Point p_n = exteriorRing.getPointN((i + nPoints) % nPoints);
			Point prev;
			int prev_index = (i - 1 + nPoints) % nPoints;
			if (outRing.isEmpty()) {
				prev = exteriorRing.getPointN(prev_index);
			} else {
				prev = GF.createPoint(outRing.get(outRing.size() - 1));
			}
			int next_index = (i + 1 + nPoints) % nPoints;
			Point next = exteriorRing.getPointN(next_index);
			LOGGER.fine(prev_index + " " + i + " " + next_index);
			LOGGER.fine(prev + " " + p_n + " " + next);
			if (p_n.equals(prev) || p_n.equals(next)) {
				LOGGER.fine("equals prev_point");
				remove = true;
			}
			if (!remove) {
				double area = Triangle.area(prev.getCoordinate(), p_n.getCoordinate(), next.getCoordinate());
				double angle = Angle.angleBetween(prev.getCoordinate(), p_n.getCoordinate(), next.getCoordinate());
				double dist1 = prev.distance(p_n);
				double angle1 = Angle.angleBetween(p_n.getCoordinate(), next.getCoordinate(), prev.getCoordinate());
				double dist2 = p_n.distance(next);
				double angle2 = Angle.angleBetween(p_n.getCoordinate(), prev.getCoordinate(), next.getCoordinate());

				if (area < area_threshold) {

					if (angle <= angle_threshold) {
						LOGGER.fine("small " + area + " < " + angle_threshold);
						LOGGER.fine("too sharp " + angle + " <= " + angle_threshold);
						remove = true;
					} else if (dist1 <= point_distance && angle1 <= angle_threshold) {
						LOGGER.fine("small " + area + " < " + angle_threshold);
						LOGGER.fine("previous sharp " + dist1 + "<=" + point_distance + "&&" + angle1 + "<="
								+ angle_threshold);
						remove = true;
					} else if (dist2 <= point_distance && angle2 <= angle_threshold) {
						LOGGER.fine("small " + area + " < " + angle_threshold);
						LOGGER.fine(
								"next sharp " + dist2 + "<=" + point_distance + "&&" + angle2 + "<=" + angle_threshold);
						remove = true;
					}
				}
				if (area < area_null) {
					LOGGER.fine("too small " + area + " < " + area_null);
					remove = true;
				}
			}
			if (!remove) {
				outRing.add(p_n.getCoordinate());
			}
		}
		// close the ring
		if (!outRing.isEmpty() && outRing.get(0) != outRing.get(outRing.size() - 1)) {
			outRing.add(outRing.get(0));
		}
		LOGGER.fine(outRing.toString());
		return outRing;
	}

	/**
	 * @return the angle_threshold
	 */
	public double getAngle_threshold() {
		return angle_threshold;
	}

	/**
	 * @param angle_threshold
	 *            the angle_threshold to set
	 */
	public void setAngle_threshold(double angle_threshold) {
		this.angle_threshold = angle_threshold;
	}

	/**
	 * @return the point_distance
	 */
	public double getPoint_distance() {
		return point_distance;
	}

	/**
	 * @param point_distance
	 *            the point_distance to set
	 */
	public void setPoint_distance(double point_distance) {
		this.point_distance = point_distance;
	}

	/**
	 * @return the area_threshold
	 */
	public double getArea_threshold() {
		return area_threshold;
	}

	/**
	 * @param area_threshold
	 *            the area_threshold to set
	 */
	public void setArea_threshold(double area_threshold) {
		this.area_threshold = area_threshold;
	}

	/**
	 * @return the area_null
	 */
	public double getArea_null() {
		return area_null;
	}

	/**
	 * @param area_null
	 *            the area_null to set
	 */
	public void setArea_null(double area_null) {
		this.area_null = area_null;
	}

	/**
	 * @param gF
	 *            the gF to set
	 */
	public void setGeometryFactory(GeometryFactory gF) {
		GF = gF;
	}

	public Geometry despike(Geometry in) {
		if (in == null || in.isEmpty() || in instanceof Point|| in instanceof MultiPoint) {
			return in;
		}
		if (in instanceof Polygon) {
			return despike((Polygon) in);
		}
		if (in instanceof LineString) {
			return GF.createLineString((cleanRing((LineString) in, false).toArray(new Coordinate[] {})));
		}
		if(in instanceof MultiPolygon || in instanceof MultiLineString || in instanceof GeometryCollection) {
			int n = in.getNumGeometries();
			ArrayList<Geometry> geomList = new ArrayList<>();
			for(int i=0;i<n;i++) {
				Geometry g = in.getGeometryN(i);
				Geometry despiked = despike(g);
				if(despiked!=null)
					geomList.add(despiked);
			}
			return GF.buildGeometry(geomList);
		}
		System.err.println("unexpected geometry "+in);
		return null;
		
	}
}

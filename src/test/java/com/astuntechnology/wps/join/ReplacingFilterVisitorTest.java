package com.astuntechnology.wps.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;

public class ReplacingFilterVisitorTest {
  static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

  @Test
  public void testVisitPropertyNameObject() {
    String[] aliases = { "", "alias" };
    String[] names = { "simple", "Capital", "ALLCAPS" };
    for (String alias : aliases) {

      for (String name : names) {
        ReplacingFilterVisitor visitor = new ReplacingFilterVisitor();
        visitor.setAlias(alias);
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("test");
        builder.add(name, String.class);
        SimpleFeatureType schema = builder.buildFeatureType();
        SimpleFeatureBuilder fbuilder = new SimpleFeatureBuilder(schema);
        fbuilder.set(name, "testValue");
        SimpleFeature f = fbuilder.buildFeature("fid");
        visitor.setFeature(f);
        if(!alias.isEmpty()) {
          name = alias+"."+name;
        }
        visitor.addFixedProperty(name);
        Expression e = (Expression) visitor.visit(ff.property(name), null);
        assertEquals(ff.literal("testValue"), e);
      }
    }
  }

  
  @Test
  public void testAddFixedProperty() {
    ReplacingFilterVisitor visitor = new ReplacingFilterVisitor();
    String[] names = { "simple", "Capital", "ALLCAPS", "Space in name", "alias.property" };
    for (String name : names) {
      visitor.addFixedProperty(name);
    }
    for (String name : names) {
      assertTrue(visitor.names.contains(name));
    }

  }

  @Test
  public void testSetAlias() {
    ReplacingFilterVisitor visitor = new ReplacingFilterVisitor();
    visitor.setAlias("alias");

    assertEquals("alias", visitor.alias);
  }

}

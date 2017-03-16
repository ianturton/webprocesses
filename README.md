
# Astun Technolgy Web Processing Service Processes

## Table Join

Join two datasets together using a filter to specify how the join should be made. 

		<?xml version="1.0" encoding="UTF-8"?><wps:Execute version="1.0.0" service="WPS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/wps/1.0.0" xmlns:wfs="http://www.opengis.net/wfs" xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:wcs="http://www.opengis.net/wcs/1.1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd">
		  <ows:Identifier>TableJoin:joinTables</ows:Identifier>
		  <wps:DataInputs>
		    <wps:Input>
		      <ows:Identifier>target</ows:Identifier>
		      <wps:Reference mimeType="text/xml" xlink:href="http://geoserver/wfs" method="POST">
		        <wps:Body>
		          <!-- a local WFS layer -->
		          <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:topp="http://www.openplans.org/topp">
		            <wfs:Query typeName="topp:states"/>
		          </wfs:GetFeature>
		        </wps:Body>
		      </wps:Reference>
		    </wps:Input>
		     <wps:Input>
		      <ows:Identifier>source</ows:Identifier>
		      <wps:Data>
		        <wps:ComplexData mimeType="application/json"><![CDATA[JSON Data]]></wps:ComplexData>
		      </wps:Data>
		    </wps:Input>
		    <wps:Input>
		      <ows:Identifier>join</ows:Identifier>
		      <wps:Data>
		        <wps:LiteralData>A CQL Filter that defines the join</wps:LiteralData>
		      </wps:Data>
		    </wps:Input>
		  </wps:DataInputs>
		  <wps:ResponseForm>
		    <wps:RawDataOutput mimeType="text/xml; subtype=wfs-collection/1.0">
		      <ows:Identifier>result</ows:Identifier>
		    </wps:RawDataOutput>
		  </wps:ResponseForm>
		</wps:Execute>
		
In the event of the two datasets having the same name for the join attributes it is necessary to add an alias for the target dataset in the filter. 
You need to use the full `simpleJoinTables` process, which allows you to specify an alias for use in the filter.
So the filter becomes something like `a.state = state` with the alias being `a`.
You can also (optionally) specify the properties that you want in the returned dataset and an output filter to reduce the output.
		
		<?xml version="1.0" encoding="UTF-8"?><wps:Execute version="1.0.0" service="WPS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/wps/1.0.0" xmlns:wfs="http://www.opengis.net/wfs" xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:wcs="http://www.opengis.net/wcs/1.1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd">
		  <ows:Identifier>TableJoin:simpleJoinTables</ows:Identifier>
		  <wps:DataInputs>
		    <wps:Input>
		      <ows:Identifier>target</ows:Identifier>
		      <wps:Reference mimeType="text/xml" xlink:href="http://geoserver/wfs" method="POST">
		        <wps:Body>
		          <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:topp="http://www.openplans.org/topp">
		            <wfs:Query typeName="topp:states"/>
		          </wfs:GetFeature>
		        </wps:Body>
		      </wps:Reference>
		    </wps:Input>
		    <wps:Input>
		      <ows:Identifier>source</ows:Identifier>
		      <wps:Reference mimeType="application/json" xlink:href="https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_10m_airports.geojson" method="GET"/>
		    </wps:Input>
		    <wps:Input>
		      <ows:Identifier>joinfilter</ows:Identifier>
		      <wps:Data>
		        <wps:LiteralData>a.name = name</wps:LiteralData>
		      </wps:Data>
		    </wps:Input>
		    <wps:Input>
		      <ows:Identifier>alias</ows:Identifier>
		      <wps:Data>
		        <wps:LiteralData>a</wps:LiteralData>
		      </wps:Data>
		    </wps:Input>
		    <wps:Input>
		      <ows:Identifier>propertyNames</ows:Identifier>
		      <wps:Data>
		        <wps:LiteralData>STATE_ABBR,the_geom</wps:LiteralData>
		      </wps:Data>
		    </wps:Input>
		    <wps:Input>
		      <ows:Identifier>outputfilter</ows:Identifier>
		      <wps:Data>
		        <wps:LiteralData>NAME like 'N%'</wps:LiteralData>
		      </wps:Data>
		    </wps:Input>
		  </wps:DataInputs>
		  <wps:ResponseForm>
		    <wps:RawDataOutput mimeType="text/xml; subtype=wfs-collection/1.0">
		      <ows:Identifier>result</ows:Identifier>
		    </wps:RawDataOutput>
		  </wps:ResponseForm>
		</wps:Execute>

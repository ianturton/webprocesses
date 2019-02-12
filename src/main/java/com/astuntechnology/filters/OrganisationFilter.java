package com.astuntechnology.filters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.IOUtils;
import org.geoserver.filters.GeoServerFilter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class OrganisationFilter implements GeoServerFilter {
	protected Logger LOGGER = org.geotools.util.logging.Logging.getLogger("org.geoserver.filters");
	private FilterConfig config;
	private String name;
	private String key;
	private static final String wps = "http://www.opengis.net/wps/1.0.0";
	private static final String ows = "http://www.opengis.net/ows/1.1";

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		config = filterConfig;
		key = config.getInitParameter("key");
		name = config.getInitParameter("wpsName");
		LOGGER.info("Setting up OrganisationFilter for process "+name+" replacing "+key);
	}

	@Override
	public void destroy() {
		config = null;
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {

		OrganisationRequestWrapper wrappedRequest = new OrganisationRequestWrapper((HttpServletRequest) request);
		String body = IOUtils.toString(wrappedRequest.getReader());
		wrappedRequest.resetInputStream(body.getBytes());

		if (!"".equals(body)) {
			
			LOGGER.fine("testing " + name + " with " + key);
			String param = wrappedRequest.getParameter(key);

			if (param != null && !param.isEmpty()) {
				LOGGER.fine("filtering " + name + " with " + key);

				byte[] myBuffer;
				try {
					DocumentBuilderFactory builder = DocumentBuilderFactory.newInstance();
					builder.setNamespaceAware(true);
					Document doc = builder.newDocumentBuilder().parse(wrappedRequest.getInputStream());
					Element exec = doc.getDocumentElement();

					NodeList ids = exec.getElementsByTagNameNS(ows, "Identifier");
					for (int i = 0; i < ids.getLength(); i++) {
						Node item = ids.item(i);
						String text = item.getTextContent();
						if (name.equalsIgnoreCase(text)) {
							NodeList data = exec.getElementsByTagNameNS(wps, "DataInputs");
							Element inputs = (Element) data.item(0);
							Element newChild = doc.createElementNS(wps, "Input");
							Element newId = doc.createElementNS(ows, "Identifier");
							newId.setTextContent(key);
							newChild.appendChild(newId);
							Element newData = doc.createElementNS(wps, "Data");
							newChild.appendChild(newData);
							Element newLiteral = doc.createElementNS(wps, "LiteralData");
							newLiteral.setTextContent(param);
							newData.appendChild(newLiteral);
							inputs.appendChild(newChild);
						}
					}
					ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
					Source xmlSource = new DOMSource(doc);
					Result outputTarget = new StreamResult(outputStream);
					TransformerFactory.newInstance().newTransformer().transform(xmlSource, outputTarget);
					myBuffer = outputStream.toByteArray();
					if(LOGGER.isLoggable(Level.FINE)) {
						LOGGER.fine(new String(myBuffer));
					}
					wrappedRequest.resetInputStream(myBuffer);
				} catch (SAXException | IOException | ParserConfigurationException | TransformerException
						| TransformerFactoryConfigurationError e) {
					LOGGER.log(Level.FINEST, "problem in OrganisationFilter", e);
					throw new RuntimeException(e);
				}

			}

		}
		chain.doFilter(wrappedRequest, response);
	}
}
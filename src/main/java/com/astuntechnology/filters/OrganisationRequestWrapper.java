package com.astuntechnology.filters;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.io.IOUtils;

public class OrganisationRequestWrapper extends HttpServletRequestWrapper {
	protected Logger LOGGER = org.geotools.util.logging.Logging.getLogger("org.geoserver.filters");
	private byte[] rawData;
	protected String charset;
	private HttpServletRequest myRequest;
	private ResettableServletInputStream servletStream;


	public OrganisationRequestWrapper(HttpServletRequest request) {
		    super(request);
		    this.myRequest = request;
		    this.servletStream = new ResettableServletInputStream();
		    this.charset = request.getCharacterEncoding();
		    LOGGER.finest("request qs "+request.getQueryString());
		}

	public void resetInputStream(byte[] newRawData) {
		servletStream.stream = new ByteArrayInputStream(newRawData);
	}

	@Override
	public ServletInputStream getInputStream() throws IOException {
		if (rawData == null) {
			rawData = IOUtils.toByteArray(this.myRequest.getReader());
			servletStream.stream = new ByteArrayInputStream(rawData);
		}
		return servletStream;
	}

	@Override
	public BufferedReader getReader() throws IOException {
		if (rawData == null) {
			rawData = IOUtils.toByteArray(this.myRequest.getReader());
			servletStream.stream = new ByteArrayInputStream(rawData);
		}
		return new BufferedReader(new InputStreamReader(servletStream));
	}

	@Override
	public String getParameter(String name) {
		return myRequest.getParameter(name);
	}

	@Override
	public Enumeration<String> getParameterNames() {
		return myRequest.getParameterNames();
	}

	@Override
	public String[] getParameterValues(String name) {
		return myRequest.getParameterValues(name);
	}

	@Override
	public Map<String, String[]> getParameterMap() {
		return myRequest.getParameterMap();
	}

	private class ResettableServletInputStream extends ServletInputStream {

		private InputStream stream;

		@Override
		public int read() throws IOException {
			return stream.read();
		}
	}
	

}

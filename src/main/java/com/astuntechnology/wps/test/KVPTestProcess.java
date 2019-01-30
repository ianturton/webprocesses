package com.astuntechnology.wps.test;

import java.util.logging.Logger;

import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.util.logging.Logging;
import org.opengis.util.InternationalString;



public class KVPTestProcess extends StaticMethodsProcessFactory<KVPTestProcess> {
	private static final Logger LOGGER = Logging.getLogger(com.astuntechnology.wps.test.KVPTestProcess.class);

	public KVPTestProcess() {
		this(Text.text("KVPTest"), "KVPTest", KVPTestProcess.class);
	}
	public KVPTestProcess(InternationalString title, String namespace, Class<KVPTestProcess> targetClass) {
		super(title, namespace, targetClass);
		
	}
	
	@DescribeProcess(title="KVPTest", description="Tests KVP params while getting a post")
	@DescribeResult(type=Integer.class, name="result",description="the number 42")
	static public int kvpTest(@DescribeParameter(name="time",description="number of seconds to sleep",min=1,max=1,minValue=0)int seconds,
			@DescribeParameter(name="fred",description="a 2nd parameter",min=0,max=1) String fred
		) {
		LOGGER.info("fred is '"+fred+')');
		try {
			Thread.sleep(seconds*1000);
		} catch (InterruptedException e) {
			// OK
		}
		return 42;
	}
}

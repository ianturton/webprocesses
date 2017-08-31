package com.astuntechnology.wps.test;

import java.util.logging.Logger;

import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.util.logging.Logging;
import org.opengis.util.InternationalString;



public class SleepProcess extends StaticMethodsProcessFactory {
	private static final Logger LOGGER = Logging.getLogger(com.astuntechnology.wps.test.SleepProcess.class);

	public SleepProcess() {
		this(Text.text("Sleep"), "Sleep", SleepProcess.class);
	}
	public SleepProcess(InternationalString title, String namespace, Class targetClass) {
		super(title, namespace, targetClass);
		
	}
	
	@DescribeProcess(title="Sleep", description="Sleeps for a number of seconds")
	static void sleeper(@DescribeParameter(name="time",defaultValue="60",description="number of seconds to sleep (default 60)",min=0,max=1,minValue=0) 
		int seconds) {
		try {
			Thread.sleep(seconds*1000);
		} catch (InterruptedException e) {
			// OK
		}
	}
}

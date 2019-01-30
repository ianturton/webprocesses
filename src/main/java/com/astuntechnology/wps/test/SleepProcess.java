package com.astuntechnology.wps.test;

import java.util.logging.Logger;

import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.factory.StaticMethodsProcessFactory;
import org.geotools.text.Text;
import org.geotools.util.logging.Logging;
import org.opengis.util.InternationalString;



public class SleepProcess extends StaticMethodsProcessFactory<SleepProcess> {
	private static final Logger LOGGER = Logging.getLogger(com.astuntechnology.wps.test.SleepProcess.class);

	public SleepProcess() {
		this(Text.text("Sleep"), "Sleep", SleepProcess.class);
	}
	public SleepProcess(InternationalString title, String namespace, Class<SleepProcess> targetClass) {
		super(title, namespace, targetClass);
		
	}
	
	@DescribeProcess(title="Sleep", description="Sleeps for a number of seconds")
	@DescribeResult(type=Integer.class, name="result",description="the number 42")
	static public int sleeper(@DescribeParameter(name="time",description="number of seconds to sleep",min=1,max=1,minValue=0) 
		int seconds) {
		try {
			Thread.sleep(seconds*1000);
		} catch (InterruptedException e) {
			// OK
		}
		return 42;
	}
}

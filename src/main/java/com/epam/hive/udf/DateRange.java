package com.epam.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * User-defined function to convert range of date [startDate, endDate]
 * to array of all dates between
 */
public final class DateRange extends GenericUDF
{

	StringObjectInspector argumentOI;
	Converter[] converters;
	List<Object> dates = new ArrayList<>();

	private List getDatesArrayFromInterval(DateTime start,
			DateTime end, DateTimeFormatter formatter) {
		
		dates.clear();
		while (start.isBefore(end) || start.isEqual(end)) {
			Text dateAsText = new Text(start.toString(formatter));
			dates.add(dateAsText);
			start = start.plusDays(1);
		}
		return dates;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
		if(objectInspectors.length != 3) {
			throw new UDFArgumentException("All three params is required: startDate, endDate and dateFormat");
		}

		if (!(objectInspectors[0] instanceof StringObjectInspector)
				|| !(objectInspectors[1] instanceof StringObjectInspector)
				|| !(objectInspectors[2] instanceof StringObjectInspector)) {
			throw new UDFArgumentException("All three params should be of type STRING");
		}

		this.argumentOI = (StringObjectInspector) objectInspectors[0];
		this.converters = new Converter[objectInspectors.length];
		converters[0] = ObjectInspectorConverters.getConverter(argumentOI, PrimitiveObjectInspectorFactory.writableStringObjectInspector);

		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		String start = argumentOI.getPrimitiveJavaObject(deferredObjects[0].get());
		String end = argumentOI.getPrimitiveJavaObject(deferredObjects[1].get());
		String dateFormat = argumentOI.getPrimitiveJavaObject(deferredObjects[2].get());

		if (start == null || end == null || dateFormat == null) {
			return null;
		}

		DateTimeFormatter formatter = DateTimeFormat.forPattern(dateFormat);
		DateTime startDate = null;
		DateTime endDate = null;
		try {
			startDate = formatter.parseDateTime(start);
			endDate = formatter.parseDateTime(end);
		} catch (IllegalArgumentException e) {
			System.err.println("ERROR: DateRangeToDateArray - can't parse start or end date: " + startDate + " " + endDate);
			startDate = null;
			endDate = null;
		}

		if (startDate == null || endDate == null || endDate.isBefore(startDate)) {
			return null;
		}

		return getDatesArrayFromInterval(startDate, endDate, formatter);
	}

	@Override
	public String getDisplayString(String[] strings) {
		return "Convert range of date [startDate, endDate] to array of all dates between including borders";
	}
}

package com.epam.hive.udf;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DateRangeTest {
	
	public static final String DATE_1 = "2016-01-01";
	public static final String DATE_2 = "2016-01-02";
	public static final String DATE_3 = "2016-01-03";
	public static final String DATE_4 = "2016-01-04";
	public static final String DATE_5 = "2016-01-05";
	public static final String DATE_6 = "2016-01-06";
	public static final String DATE_1_FORMAT_2 = "2016-01-06 23:44";
	public static final Text TEXT_DATE_1 = new Text(DATE_1);
	public static final Text TEXT_DATE_1_FORMAT_2 = new Text(DATE_1_FORMAT_2);
	public static final Text TEXT_DATE_2 = new Text(DATE_2);
	public static final Text TEXT_DATE_3 = new Text(DATE_3);
	public static final Text TEXT_DATE_4 = new Text(DATE_4);
	public static final Text TEXT_DATE_5 = new Text(DATE_5);
	public static final Text TEXT_DATE_6 = new Text(DATE_6);
	public static final Text RIGHT_DATE_FORMAT = new Text("yyyy-MM-dd");
	public static final Text WRONG_DATE_FORMAT = new Text("yyyy-MM-dd HH:mm");

	public static final StringObjectInspector STRING_OBJ_INSPECTOR = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	public static final StandardListObjectInspector LIST_OBJ_INSPECTOR = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

	public static List<Text> datesList;
	
	DateRange testObj;
    
	@Before
	public void setup() throws Exception{
		testObj = new DateRange();
		testObj.initialize(new ObjectInspector[] {STRING_OBJ_INSPECTOR, STRING_OBJ_INSPECTOR ,STRING_OBJ_INSPECTOR});

		datesList = new ArrayList();
		datesList.add(TEXT_DATE_1);
		datesList.add(TEXT_DATE_2);
		datesList.add(TEXT_DATE_3);
		datesList.add(TEXT_DATE_4);
		datesList.add(TEXT_DATE_5);
		datesList.add(TEXT_DATE_6);
	}
	
	@Test
	public void evaluateHappyDayTest() throws Exception {
		//given
		DeferredObject startDate = new DeferredJavaObject(TEXT_DATE_1);
		DeferredObject endDate = new DeferredJavaObject(TEXT_DATE_6);
		DeferredObject dateFormat = new DeferredJavaObject(RIGHT_DATE_FORMAT);
		
		//when
		Object datesArray = testObj.evaluate(new DeferredObject[]{startDate, endDate, dateFormat});
		
		//then
		assertEquals(LIST_OBJ_INSPECTOR.getListLength(datesArray), 6);
		assertEquals(datesList, LIST_OBJ_INSPECTOR.getList(datesArray));
	}

	@Test
	public void evaluateWrongFormatTest() throws Exception {
		//given
		DeferredObject startDate = new DeferredJavaObject(TEXT_DATE_1);
		DeferredObject endDate = new DeferredJavaObject(TEXT_DATE_6);
		DeferredObject dateFormat = new DeferredJavaObject(WRONG_DATE_FORMAT);

		//when
		Object datesArray = testObj.evaluate(new DeferredObject[]{startDate, endDate, dateFormat});

		//then
		assertNull(datesArray);
	}

	@Test
	public void evaluateEndBeforeStartDateTest() throws Exception {
		//given
		DeferredObject startDate = new DeferredJavaObject(TEXT_DATE_6);
		DeferredObject endDate = new DeferredJavaObject(TEXT_DATE_1);
		DeferredObject dateFormat = new DeferredJavaObject(RIGHT_DATE_FORMAT);

		//when
		Object datesArray = testObj.evaluate(new DeferredObject[]{startDate, endDate, dateFormat});

		//then
		assertNull(LIST_OBJ_INSPECTOR.getList(datesArray));
	}

	@Test
	public void evaluateStartDateNullTest() throws Exception {
		//given
		DeferredObject startDate = new DeferredJavaObject(null);
		DeferredObject endDate = new DeferredJavaObject(TEXT_DATE_6);
		DeferredObject dateFormat = new DeferredJavaObject(RIGHT_DATE_FORMAT);

		//when
		Object datesArray = testObj.evaluate(new DeferredObject[]{startDate, endDate, dateFormat});

		//then
		assertNull(LIST_OBJ_INSPECTOR.getList(datesArray));
	}

	@Test
	public void evaluateEndDateNullTest() throws Exception {
		//given
		DeferredObject startDate = new DeferredJavaObject(TEXT_DATE_1);
		DeferredObject endDate = new DeferredJavaObject(null);
		DeferredObject dateFormat = new DeferredJavaObject(RIGHT_DATE_FORMAT);

		//when
		Object datesArray = testObj.evaluate(new DeferredObject[]{startDate, endDate, dateFormat});

		//then
		assertNull(LIST_OBJ_INSPECTOR.getList(datesArray));
	}

	@Test
	public void evaluateStartDateWrongFormatTest() throws Exception {
		//given
		DeferredObject startDate = new DeferredJavaObject(TEXT_DATE_1_FORMAT_2);
		DeferredObject endDate = new DeferredJavaObject(TEXT_DATE_6);
		DeferredObject dateFormat = new DeferredJavaObject(RIGHT_DATE_FORMAT);

		//when
		Object datesArray = testObj.evaluate(new DeferredObject[]{startDate, endDate, dateFormat});

		//then
		assertNull(LIST_OBJ_INSPECTOR.getList(datesArray));
	}

	@Test
	public void evaluateEndDateWrongFormatTest() throws Exception {
		//given
		DeferredObject startDate = new DeferredJavaObject(TEXT_DATE_1);
		DeferredObject endDate = new DeferredJavaObject(TEXT_DATE_1_FORMAT_2);
		DeferredObject dateFormat = new DeferredJavaObject(RIGHT_DATE_FORMAT);

		//when
		Object datesArray = testObj.evaluate(new DeferredObject[]{startDate, endDate, dateFormat});

		//then
		assertNull(LIST_OBJ_INSPECTOR.getList(datesArray));
	}
}

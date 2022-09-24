package com.sparksql.util.schemacustomdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

public class StructFieldsUtil {

	public static StructField[] getSampleStructFields() {

		StructField[] fields = new StructField[] {
				new StructField("loglevel", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()) 
		};
		return fields;
	}
	
	public static List<Row> inMemoryRowData() {

		List<Row> inMemoryData = new ArrayList<>();
		inMemoryData.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemoryData.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemoryData.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemoryData.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemoryData.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));
		inMemoryData.add(RowFactory.create("WARN", "2016-12-30 04:18:31"));
		inMemoryData.add(RowFactory.create("FATAL", "2016-12-30 03:21:33"));
		inMemoryData.add(RowFactory.create("WARN", "2016-12-30 03:20:20"));
		inMemoryData.add(RowFactory.create("INFO", "2015-4-20 14:31:20"));
		inMemoryData.add(RowFactory.create("FATAL", "2015-4-20 19:22:19"));
		
		return inMemoryData;
	}
}

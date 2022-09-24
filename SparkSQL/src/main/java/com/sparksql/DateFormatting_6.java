package com.sparksql;

import java.time.Duration;
import java.time.Instant;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

public class DateFormatting_6 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();

//		List<Row> inMemoryData = StructFieldsUtil.inMemoryRowData();
//		StructField[] fields = StructFieldsUtil.getSampleStructFields();
		
		Dataset<Row> logData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/biglog.txt");
		logData.createOrReplaceTempView("logging_table");
		
		Dataset<Row> result = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total "
				+ "from logging_table group by level, month");
		
//		result.createOrReplaceTempView("logging_table");
//		result = sparkSession.sql("select sum(total) from logging_table");
		
		
		
		Instant start = Instant.now();
		System.out.println("Start time: " + DateTime.now());
		result.show();	
		System.out.println("End time: " + DateTime.now());
		Instant end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		System.out.println("Total time taken: " + timeElapsed.toMillis() +" milliseconds");
		sparkSession.close();
		
		
//		Instant start = Instant.now();
//		Instant end = Instant.now();
//		Duration timeElapsed = Duration.between(start, end);
//		System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

	}

}

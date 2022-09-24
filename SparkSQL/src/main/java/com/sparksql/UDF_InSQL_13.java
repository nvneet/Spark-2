package com.sparksql;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;

public class UDF_InSQL_13 {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();
		
		Dataset<Row> logData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/biglog.txt");
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		
		sparkSession.udf().register("monthNum", (String month) -> {
			java.util.Date inputMonth = input.parse(month);
			return Integer.parseInt(output.format(inputMonth));
		}, DataTypes.IntegerType);
		
		Instant start = Instant.now();
		System.out.println("Start time: " + DateTime.now());
		
		logData.createOrReplaceTempView("logging_table");
//		Dataset<Row> result = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as Total "
//				+ "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");

		Dataset<Row> result = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as Total "
				+ "from logging_table group by level, month order by monthNum(month), level");

		result.show();	
		System.out.println("End time: " + DateTime.now());
		Instant end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		System.out.println("Total time taken: " + timeElapsed.toMillis() +" milliseconds");
		sparkSession.close();
	}

}

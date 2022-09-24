package com.sparksql;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.date_format;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;

public class SparkSQL_Performance_14 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();
		
		Dataset<Row> logData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/biglog.txt");
		
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		sparkSession.udf().register("monthNum", (String month) -> {
			java.util.Date inputDate = input.parse(month);
			return Integer.parseInt(output.format(inputDate));
		}, DataTypes.IntegerType);
		
		// To compare execution time of "Spark-SQL" and "Spark DataFrame"
		Instant start = Instant.now();
		System.out.println("Start time: " + DateTime.now());
		
		
//		logData.createOrReplaceTempView("logging_table");
//		Dataset<Row> result = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total "
//				+ "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");

		
//		Dataset<Row> resultDf = logData.selectExpr("level", "date_format(datetime,'MMMM') as month");
		Dataset<Row> resultDf = logData.select(col("level"), 
										date_format(col("datetime"),"MMMM").alias("month"),
										callUDF("monthNum",date_format(col("datetime"),"MMMM")).as("monthnum")); // date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));

		resultDf = resultDf.groupBy(col("level"), col("month"), col("monthnum")).count().as("total")
																				.orderBy("monthNum", "level");

		resultDf.show(1000000);
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		

//		result.show(1000000);	
		System.out.println("End time: " + DateTime.now());
		Instant end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		System.out.println("Total time taken: " + timeElapsed.toMillis() +" milliseconds");
		sparkSession.close();

	}

}

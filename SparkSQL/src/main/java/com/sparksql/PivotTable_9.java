package com.sparksql;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class PivotTable_9 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();
		Dataset<Row> logData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/biglog.txt");

		Dataset<Row> resultDf = logData.select(col("level"), 
										date_format(col("datetime"),"MMMM").alias("month"),
										date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
//		resultDf = resultDf.groupBy(col("level"), col("month"), col("monthnum")).count();
//		resultDf = resultDf.orderBy(col("monthnum"), col("level"));
//		resultDf = resultDf.drop(col("monthnum"));
		
//		Object[] months = new Object[] {"January","February","March","April","May","June","July","August","September","October","November","December"};
//		List<Object> monthsList = Arrays.asList(months);
//		resultDf = resultDf.groupBy(col("level")).pivot("month",monthsList).count();
		
		Object[] months = new Object[] {"January","February","March","April","May","June","Junely", "July","August","September","October","November","December"};
		List<Object> monthsList = Arrays.asList(months);
		resultDf = resultDf.groupBy(col("level")).pivot("month",monthsList).count().na().fill(0);
		
		
		resultDf.show(100);
		sparkSession.close();
	}
}

package com.myspark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Sorting {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().master("local[*]")
						.appName("SS")
						.config("spark.sql.warehouse.dir","file:///C:/temp/")
						.getOrCreate();
		
		Dataset<Row> dataset = sparkSession.read().option("header", true)
				.csv("/src/main/resources/biglog.txt");
		dataset.createOrReplaceTempView("logging_table");
		
//		Dataset<Row> results = sparkSession.sql
//				("select level, date_format(datetime,'MMMM') as Month, count(1) as total " +
//		"from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");
		
		Dataset<Row> results = sparkSession.sql
				("select level, date_format(datetime,'MMMM') as month, count(1) as total " +
		"from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int)");
		
		results.show(100);
//		sparkSession.close();
		
		
		
		
		
		
		
		
		
		
		
		
		
	}
}
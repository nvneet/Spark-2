package com.sparksql;

import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PivotTableExercise_11 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("PivotTableExercise") .master("local[*]").getOrCreate();
		Dataset<Row> studentsData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/exams/students.csv");
		
		
		studentsData = studentsData.groupBy(col("subject")).pivot("year").agg(round(avg(col("score")), 2).alias("Average"),
																			 round(stddev(col("score")), 2).alias("stddev_score"));
		// GO to UDF now in 12th module
		studentsData.show();
		sparkSession.close();

	}

}

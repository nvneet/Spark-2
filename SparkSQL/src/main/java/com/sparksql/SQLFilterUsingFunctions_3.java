package com.sparksql;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQLFilterUsingFunctions_3 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();
		Dataset<Row> studentsData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/exams/students.csv");

//		Column subjectColumn = studentsData.col("subject");
//		Column yearColumn = studentsData.col("year");
//		Dataset<Row> modernArtResults = studentsData.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
			
		Dataset<Row> modernArtResults = studentsData.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
		
		modernArtResults.show();		
		sparkSession.close();

	}
}

package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlFilter_2 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();
		Dataset<Row> studentsData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/exams/students.csv");
		
//		Dataset<Row> modernArtResults = studentsData.filter("subject = 'Modern Art' AND year >= 2007");
//		Dataset<Row> modernArtResults = studentsData.filter(row -> row.getAs("subject").equals("Modern Art")
//																&& Integer.parseInt(row.getAs("year")) > 2007);
		
		Column subjectColumn = studentsData.col("subject");
		Column yearColumn = studentsData.col("year");
		Dataset<Row> modernArtResults = studentsData.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
				
		modernArtResults.show();		
		sparkSession.close();
	}
}

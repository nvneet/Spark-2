package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkTempView_4 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();
		Dataset<Row> studentsData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/exams/students.csv");

		studentsData.createOrReplaceTempView("students_view");
//		Dataset<Row> results = sparkSession.sql("select subject,score,year from students_view where subject='French'");
//		Dataset<Row> results = sparkSession.sql("select max(score) from students_view where subject='French'");
//		Dataset<Row> results = sparkSession.sql("select min(score) from students_view where subject='French'");
//		Dataset<Row> results = sparkSession.sql("select avg(score) from students_view where subject='French'");
		Dataset<Row> results = sparkSession.sql("select Distinct(year) from students_view order by year desc");

		results.show();		
		sparkSession.close();

	}

}

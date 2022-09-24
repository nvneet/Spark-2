package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main_1 {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
//		SparkConf sparkConf = new SparkConf().setAppName("SparkSQL-App").setMaster("local[*]");	
//		JavaSparkContext context = new JavaSparkContext(sparkConf);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();
		
		Dataset<Row> studentsData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/exams/students.csv");

//		Long numRows = studentsData.count();
//		System.out.println("Number of rows = " + numRows);
		
		Row students = studentsData.first();
//		String data = students.get(2).toString();
		String data = students.getAs("subject").toString();
		System.out.println("Subject: " + data);
		
		int year = Integer.parseInt(students.getAs("year"));
		System.out.println("Passing year = " + year);
		
		sparkSession.close();
	}

}



//sparkSession.read().orc("path");
//sparkSession.read().parquet("path");
//sparkSession.read().json("path");
//sparkSession.read().textFile("path");
//sparkSession.read().text("path");
//sparkSession.read().schema(/*StructType*/ "schema");
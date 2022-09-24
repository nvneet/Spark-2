package com.sparksql;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SqlToDataFrame_8 {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession sparkSession = SparkSession.builder().appName("SparkSQL-App").master("local[*]").getOrCreate();
		
		Dataset<Row> logData = sparkSession.read().option("header", true).csv("/root/sparkfiles/testdata/biglog.txt");
		
//		logData.createOrReplaceTempView("logging_table");
//		Dataset<Row> result = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total "
//				+ "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");

		
//		Dataset<Row> resultDf = logData.selectExpr("level", "date_format(datetime,'MMMM') as month");
		Dataset<Row> resultDf = logData.select(col("level"), 
										date_format(col("datetime"),"MMMM").alias("month"),
										date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
		resultDf = resultDf.groupBy(col("level"), col("month"), col("monthnum")).count();
		resultDf = resultDf.orderBy(col("monthnum"), col("level"));
		resultDf = resultDf.drop(col("monthnum"));
		resultDf.show(100);
		sparkSession.close();
	}
}

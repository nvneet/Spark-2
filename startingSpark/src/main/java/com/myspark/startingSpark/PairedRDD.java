package com.myspark.startingSpark;
//package com.myspark.rddpaired;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairedRDD {

	public static void main(String[] args) {
		
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> originalData = sc.parallelize(inputData);
		JavaPairRDD<String, Long> pairedRdd = originalData.mapToPair(row -> {
			String[] columns = row.split(":");
			String level = columns[0];
//			String date = columns[1];
//			return new Tuple2<>(level,date);
			return new Tuple2<>(level,1L);
		});
		JavaPairRDD<String, Long> statusRdd = pairedRdd.reduceByKey((count1,count2) -> count1+count2);
		statusRdd.foreach(statusRow -> System.out.println(statusRow._1 + " count is = " + statusRow._2));
		
		sc.parallelize(inputData)
		  .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0] , 1L  ))
		  .reduceByKey((value1, value2) -> value1 + value2)
		  .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		
		// groupbykey version - not recommended due to performance (see later) AND the iterable
		// is awkward to work with.

		sc.close();
	}

}
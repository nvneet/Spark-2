package com.myspark.startingSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class Main 
{
	public static void main(String[] args) 
	{
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		Integer result = myRdd.reduce((value1, value2) -> value1 + value2);
		System.out.println("result = " + result);
		
		JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
		//sqrtRdd.foreach(value -> System.out.println(value));
		sqrtRdd.collect().forEach(System.out :: println);
		
		// count number of elements using map and reduce
		JavaRDD<Long> mappedRdd = myRdd.map(value -> 1L);
		Long count = mappedRdd.reduce((value1,value2) -> value1 + value2);
		System.out.println("Count = " + count);
		
		JavaRDD<Tuple2<Integer, Double>> withSqrtRdd = myRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));
		withSqrtRdd.collect().forEach(System.out :: println); 
		
		
		
		sc.close();
    }
}

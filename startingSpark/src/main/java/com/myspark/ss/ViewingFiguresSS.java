package com.myspark.ss;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class ViewingFiguresSS {
	
	public static void main(String[] args) 
	{
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkSession session = SparkSession.builder().master("local[*]").appName("SS").getOrCreate();
		
		session.conf().set("spark.dynamicAllocation.enabled", "true");
		session.conf().set("spark.executor.cores", 4);
		session.conf().set("spark.dynamicAllocation.minExecutors","1");
		session.conf().set("spark.dynamicAllocation.maxExecutors","5");
		
		session.conf().set("spark.sql.shuffle.partitions", "10");
		
		Dataset<Row> df = session.readStream().format("kafka")
							     .option("kafka.bootstrap.servers", "localhost:9092")
								 .option("subscribe", "mytopic")
								 .load();
		
		// start dataframe operations
		df.createOrReplaceTempView("viewing_figures");
		
		// key, value, Timestamp
		Dataset<Row> results = session.sql("select window, cast(value as string) as course_name, sum(5) "
				+ "from viewing_figures group by window(timestamp, '2 minutes'), course_name");
		
		StreamingQuery query = results.writeStream().format("console")
													.outputMode(OutputMode.Append())
//													.trigger(Trigger.ProcessingTime(Durations.seconds(5))
													.start();
		query.awaitTermination();
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(30));
		JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8080);
		
		JavaPairDStream<String, Long> pairData = inputData.map(item -> item)
														  .mapToPair(ro -> new Tuple2<> (ro.split(",")[0], 1L))
														  .reduceByKey((x,y) -> x+y);
														  
		
		
		
		
		
		
		
		
		
		
	}
}
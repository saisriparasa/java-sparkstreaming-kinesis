package com.java_mvn_sparkstreaming.SparkStreaming;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.*;

import scala.Tuple2;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.collect.Lists;

/**
 * Hello world!
 *
 */
public class JavaSparkStreaming 
{
    protected static final Pattern WORD_SEPARATOR = Pattern.compile(" ");

	public static void main( String[] args )
    {
        String streamName = "js-test";
        String endpointUrl = "https://kinesis.us-west-2.amazonaws.com";
        
        Duration batchInterval = new Duration(2000);
        AmazonKinesisClient kinesisClient = new AmazonKinesisClient(
        		new DefaultAWSCredentialsProviderChain());
        
        kinesisClient.setEndpoint(endpointUrl);
        
        int numShards = kinesisClient.describeStream(streamName)
        		.getStreamDescription().getShards().size();
        
        int numStreams = numShards;
        
        /* Setup the Spark config. */
        SparkConf sparkConfig = new SparkConf().setAppName("KinesisWordCount");
        
        /* Kinesis checkpoint interval. Same as batchInterval for this example. */
        Duration checkpointInterval = batchInterval;
        
        /* Setup the StreamingContext */
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);
        
        /* Create the same number of Kinesis DStreams/Receivers as Kinesis stream's shards */
        List<JavaDStream<byte[]>> streamsList = new ArrayList<JavaDStream<byte[]>>(numStreams);
        
        for (int i = 0; i < numStreams; i++) {
	        streamsList.add(
		        KinesisUtils.createStream(jssc, streamName, endpointUrl, checkpointInterval,
		        InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2())
	        );
        }
        

		/* Union all the streams if there is more than 1 stream */
		JavaDStream<byte[]> unionStreams;
		if (streamsList.size() > 1) {
			unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
		} else {
			/* Otherwise, just use the 1 stream */
			unionStreams = streamsList.get(0);
		}

		JavaDStream<String> words = unionStreams.flatMap(new FlatMapFunction<byte[], String>() {
			public Iterable<String> call(byte[] line) {
			return Lists.newArrayList(WORD_SEPARATOR.split(new String(line)));
			}
		});

		 /* Map each word to a (word, 1) tuple, then reduce/aggregate by word. */
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
			new PairFunction<String, String, Integer>() {
				public Tuple2<String, Integer> call(String s) {
					return new Tuple2<String, Integer>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		
		wordCounts.print();
		/* Start the streaming context and await termination */
		System.out.println("\n\n\n====================\n\n\n");
		System.out.println("Starting");
		System.out.println("\n\n\n====================\n\n\n");
		
		jssc.start();
		
		jssc.awaitTermination(20000);
		System.out.println("\n\n\n====================\n\n\n");
		System.out.println("Stopping");
		jssc.stop();
		System.out.println("\n\n\n====================\n\n\n");
		        
    }
}

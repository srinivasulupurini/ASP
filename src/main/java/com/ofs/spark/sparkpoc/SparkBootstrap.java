package com.ofs.spark.sparkpoc;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;



/**
 * Spark BootStrap
 *
 */
/**
 * @author Srini
 *
 */
public class SparkBootstrap 
{
    public static void main( String[] args )
    {
    	//appName parameter is a name for your application to show on the cluster UI
    	String appName = "SparkPoc";
    	//master is a Spark, Mesos or YARN cluster URL, or a special “local”
    	//string to run in local mode
    	String masterName="local";
    	
    	SparkConf conf = new SparkConf().setAppName(appName).setMaster(masterName);
    	JavaSparkContext sc = new JavaSparkContext(conf); 
    	JavaRDD<String> lines = sc.textFile("SparkInput.txt");
    	JavaPairRDD<String, Integer> ones = lines.mapToPair(line ->  new Tuple2<String, Integer>(line, 1));
    	JavaPairRDD<String, Integer> counts = ones.reduceByKey((x, y) -> x + y); 
    	
    	List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
          System.out.println(tuple._1() + ": " + tuple._2());
        }
        sc.stop();
    	
    	
    	
    	
    	
    }
}

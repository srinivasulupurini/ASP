package com.ofs.spark.sparkpoc;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;



/**
 * Spark BootStrap sample2
 *
 */
/**
 * @author Srini
 *
 */
public class Sample2 

{
	//set jvm arguments
	//-Dspark.app.name=SparkPoc -Dspark.master=local
	
	//Sample input file format:
	//year passed,name,state,age,percentage
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
    	
    	//getting line count
    	long rowCount = lines.count();
    	System.out.println("Row Count::"+rowCount);
    	
    	
    	//getting first row
    	String firstElement = lines.first();
    	System.out.println("First Element"+firstElement);
    	
    	//splitting the rows into array of strings
    	JavaRDD<String[]> rows = lines.map(line -> line.split(","));
    	
    	//get the distinct count of names in input
    	long nameCount = rows.map(row -> row[1]).distinct().count();
    	System.out.println("name count"+nameCount);
    	
    	//filter rows based on name 
    	JavaRDD<String[]> nameFilter = rows.filter(row -> row[1].contains("ADDISON"));
    	System.out.println("filter for ADDISON :"+nameFilter.count()+" rows found");
    	
    	//find people with age>28
    	JavaRDD<String[]> ageFilter = rows.filter(row -> Integer.parseInt(row[3])>28);
    	System.out.println("filter for AGE>28 :"+ageFilter.count()+" rows found");
    	
    	//sorting name tuples
    	JavaPairRDD<String, Integer> names = rows.mapToPair(name ->  new Tuple2<String, Integer>(name[1], 1));
    	JavaPairRDD<String, Integer> namesByKey = names.reduceByKey((x,y) -> (x+y));
    	JavaPairRDD<String, Integer> sortedNameTuples = namesByKey.sortByKey(false);
    	
    	List<Tuple2<String, Integer>> output = sortedNameTuples.collect();
        for (Tuple2<?,?> tuple : output) {
          System.out.println(tuple._1() + ": " + tuple._2());
        }
    	
        sc.stop();
       }
}

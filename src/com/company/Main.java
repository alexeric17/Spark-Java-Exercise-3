package com.company;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.PairRDDFunctions;
import scala.Tuple2;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;

import static org.apache.avro.TypeEnum.b;

public class Main {

    public static void main(String[] args) {
        /*
        A measurement method records the surface of objects as a set of 3D vectors and stores them in the
        database table S(vectorID, dimension, value). Due to measurement problems, not all
        vectors are completely recorded so that some dimensions are missing. The task is to compute the
        vector lengths of all fully captured vectors: length = x2 + y2 + z2.
        Use Spark to create a table T(vectorId, length) in which the length of each fully captured
        vector is stored. An example input file is given in Digicampus. Save the resulting table of vector
        lenghts to a CSV file and output it onto the console.
         */

        System.setProperty("hadoop.home.dir", "C:\\winutil\\");

        //Config spark
        SparkConf conf = new SparkConf().setAppName("WordCounting").setMaster("local");
        //Start spark conext
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputFile = sc.textFile("C:\\Users\\Alexander Eric\\Desktop\\Skola augsburg\\JavaSpark\\Spark-Java-Exercise-3\\vectors.csv");
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        //Map Tuple2<ID, <dimension, value>>.
        //Realized I could have saved by ID, value. And if there is 3 values for the ID we can then
        JavaPairRDD<Integer,Double> pairtest = wordsFromFile.mapToPair(ID -> {
            String[] split = ID.split(",");
            //We dont have to care about x,y,z specifically. All we need is 3 values for each ID.
                Tuple2<Integer,Double> res = new Tuple2<Integer,Double>(Integer.parseInt(split[0]),Double.parseDouble(split[2]));
                //this will give us an ID and a value.
            return res;
        });

        //groupByKey, maybe there is a better solution?
        //Tuple2(ID, [value1,value2,value3]), if we can check how big the array is we can
        JavaPairRDD result = pairtest.groupByKey();

        //If size of list
        Function<Tuple2<Integer,Iterable<Double>>,Boolean> filterFunction = w -> {
            int count = 0;
            for(Object i: w._2()){
                count++;
            }
            if (count<3){
                return true;
            }
            return false;
        };
        JavaPairRDD<Integer,Iterable<Double>> filterRDD = result.filter(filterFunction);
        JavaPairRDD<Integer,Iterable<Double>> xyzRDD = result.subtract(filterRDD);

        //xyzRDD has all the vectors with all 3 variables.
        Function<Tuple2<Integer,Iterable<Double>>,Tuple2<Integer,Double>> calcFunction = w -> {
            double r = 0;
            for(Double i: w._2()){
                r += i*i;
            }
            //Return tuple with ID,sqrt(x**2 + y**2 + z**2)
            return new Tuple2<Integer,Double> (w._1(),Math.sqrt(r));
        };

        JavaPairRDD<Integer,Double> result1 = xyzRDD.flatMapToPair(calcFunction);

        result1.saveAsTextFile("resultData");

    }
}


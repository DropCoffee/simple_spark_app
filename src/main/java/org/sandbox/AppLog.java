package org.sandbox;

import com.google.common.collect.Iterators;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AppLog {
    public static void main(String[] args) {
        List<String> logMessages = new ArrayList<>();
        logMessages.add("WARN: Tuesday 4 September 0405");
        logMessages.add("ERROR: Tuesday 4 September 0408");
        logMessages.add("FATAL: Wednesday 5 September 1632");
        logMessages.add("ERROR: Friday 7 September 1854");
        logMessages.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf().setAppName("first app").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(logMessages)
                .mapToPair(rowValue -> new Tuple2<>(rowValue.split(":")[0], 1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instance"));

        // same, but groupByKey
        sc.parallelize(logMessages)
                .mapToPair(rowValue -> new Tuple2<>(rowValue.split(":")[0], 1L))
                .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterators.size(tuple._2.iterator())));

        sc.close();
    }
}

package org.sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        List<Double> inputData = new ArrayList<>();
        inputData.add(100.5);
        inputData.add(100.5);
        inputData.add(24.3);
        inputData.add(26.7);
        System.out.println(inputData);

        SparkConf conf = new SparkConf().setAppName("first app").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        double result = myRdd.reduce((x1, x2) -> x1 + x2);

        System.out.println(result);

        sc.close();
    }
}

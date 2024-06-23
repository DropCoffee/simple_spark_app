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

        List<Integer> inputData = new ArrayList<>();
        inputData.add(100);
        inputData.add(100);
        inputData.add(24);
        inputData.add(26);
        System.out.println(inputData);

        SparkConf conf = new SparkConf().setAppName("first app").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        int result = myRdd.reduce((x1, x2) -> x1 + x2);

        System.out.println(result);

        JavaRDD<Double> sqrtRdd = myRdd.map(x1 -> Math.sqrt(x1));

        sqrtRdd.foreach(val -> System.out.println(val));

        // count elements
        System.out.println(sqrtRdd.count());
        // using map
        JavaRDD<Long> singleRdd = sqrtRdd.map(val -> 1L);
        long count = singleRdd.reduce((x, y) -> x + y);

        // problem with ser
        singleRdd.collect().forEach(System.out::println);

        System.out.println(count);

        sc.close();
    }
}
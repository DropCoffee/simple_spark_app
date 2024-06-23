package org.sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AppTuples {
    private static class IntegerWithSquareRoot {
        private int originalNumber;
        private double squareRootNumber;

        public IntegerWithSquareRoot(int number) {
            this.originalNumber = number;
            this.squareRootNumber = Math.sqrt(originalNumber);
        }
    }

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        List<Integer> inputData = new ArrayList<>();
        inputData.add(100);
        inputData.add(100);
        inputData.add(24);
        inputData.add(26);

        SparkConf conf = new SparkConf().setAppName("tuples app").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);
        JavaRDD<IntegerWithSquareRoot> sqrtRddJL = myRdd.map(x1 -> new IntegerWithSquareRoot(x1));

        JavaRDD<Tuple2<Integer, Double>> sqrtRddSL = myRdd.map(x1 -> new Tuple2<>(x1, Math.sqrt(x1)));

        sc.close();
    }
}


package org.sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class AppWords {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");

        SparkConf conf = new SparkConf().setAppName("first app").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        List<Tuple2<Long, String>> words = initialRdd
                .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .filter(sentence -> sentence.trim().length() > 0)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(word -> word.trim().length() > 0)
                .filter(word -> Util.isNotBoring(word))
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10);

        words.forEach(word -> System.out.println(word));

        sc.close();
    }
}

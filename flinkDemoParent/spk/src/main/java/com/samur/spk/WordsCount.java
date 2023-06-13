package com.samur.spk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordsCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordsCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("D:\\project\\flinkDemo\\flinkDemoParent\\spk\\src\\main\\resources\\input.txt")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a+b)
                .foreach(e -> System.out.println(e._1() + ":" + e._2()));

        sc.stop();
    }
}

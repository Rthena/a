package com.ibeifeng.sparkproject.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public class RDDTest {
    public static void main(String[] args) {

    }

    public static void test1(){
        final HashMap<String, Map<String, List<Integer>>> m = new HashMap<>();
        final Set<Map.Entry<String, Map<String, List<Integer>>>> entries = m.entrySet();
        for (Map.Entry<String, Map<String, List<Integer>>> mm:
        entries) {
            mm.getKey();
        }
    }

    public static void test0() {
        final SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RDDTest");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> linesRDD = sc.textFile("F:\\test.txt");

        final JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });


        final JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                final Tuple2<String, Integer> tuple2 = new Tuple2<>(s, 1);
                return tuple2;
            }
        });

        final JavaPairRDD<String, Integer> wordCount = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        final List<Tuple2<String, Integer>> collect = wordCount.collect();
        System.out.println(collect);

        while (true) {}
    }
}


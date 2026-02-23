package com.spark.probando;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CreacionRDDs {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("RDDS")
                .master("local[*]")
                .getOrCreate();

        try (JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext())) {

            List<Integer> numeros = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
            JavaRDD<Integer> rddNumeros = sc.parallelize(numeros);

            rddNumeros.collect().forEach(System.out::println);
        }

        sparkSession.stop();
    }
}

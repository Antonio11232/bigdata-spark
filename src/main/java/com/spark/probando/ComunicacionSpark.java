package com.spark.probando;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ComunicacionSpark {


    public static void main(String[] args) {


        SparkSession sparkSession = SparkSession.builder()
                .appName("RDD Debug Example")
                .master("local[*]")
                .getOrCreate();

        try (JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext())) {


            List<String> lineas = Arrays.asList(
                    "hola mundo hola",
                    "adios mundo",
                    "hola spark",
                    "spark es divertido"
            );
            JavaRDD<String> rddLineas = sc.parallelize(lineas, 2); // 2 particiones para debug


            System.out.println("===== RDD inicial =====");
            rddLineas.foreachPartition(part -> {
                System.out.println("Worker ejecutando partición: " + part);
                part.forEachRemaining(System.out::println);
            });


            JavaRDD<String> rddPalabras = rddLineas.flatMap(linea -> Arrays.asList(linea.split(" ")).iterator());


            System.out.println("===== Después de flatMap =====");
            rddPalabras.foreachPartition(part -> {
                System.out.println("Worker ejecutando partición: " + part);
                part.forEachRemaining(System.out::println);
            });


            JavaPairRDD<String, Integer> pares = rddPalabras.mapToPair(p -> new Tuple2<>(p, 1));


            JavaPairRDD<String, Integer> conteo = pares.reduceByKey(Integer::sum);


            System.out.println("===== Conteo final de palabras =====");
            conteo.collect().forEach(t -> System.out.println(t._1 + " -> " + t._2));

        }

        sparkSession.stop();
    }

}

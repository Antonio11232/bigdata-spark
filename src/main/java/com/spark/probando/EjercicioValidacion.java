package com.spark.probando;

import org.apache.spark.sql.SparkSession;

public class EjercicioValidacion {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("probandoInstalacion")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Version de spark: " + spark.version());
    }
}

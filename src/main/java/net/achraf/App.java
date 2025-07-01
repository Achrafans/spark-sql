package net.achraf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class App {
    public static void main(String[] args) {
        // Initialisation de Spark
        SparkSession spark = SparkSession.builder()
                .appName("TP3 Spark SQL - Incidents")
                .master("local[*]") // pour exécution locale
                .getOrCreate();

        // Chargement du fichier CSV
        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("incidents.csv");

        // Affichage du schéma
        df.printSchema();

        // 1. Nombre d’incidents par service
        System.out.println("Nombre d'incidents par service :");
        df.groupBy("service")
                .count()
                .orderBy(col("count").desc())
                .show();

        // 2. Les deux années avec le plus d’incidents

        // Conversion de la date au bon format
        Dataset<Row> dfAvecAnnee = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
                .withColumn("annee", year(col("date")));

        System.out.println("Les deux années avec le plus d'incidents :");
        dfAvecAnnee.groupBy("annee")
                .count()
                .orderBy(col("count").desc())
                .limit(2)
                .show();

        spark.stop();

    }
}

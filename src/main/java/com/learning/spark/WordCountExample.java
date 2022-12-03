package com.learning.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Word Count App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String path = "src/main/resources/avatar-wiki.txt";

        JavaRDD<String> avatarRDD = sc.textFile(path);
        //Remove non charater words
        JavaRDD<String> wordsAvatarRDD = avatarRDD.map(word -> word.replaceAll("[^A-Za-z\\s]", "").toLowerCase());
        // Remove blank lines
        JavaRDD<String> removedBlankLines = wordsAvatarRDD.filter(sentence -> sentence.trim().length() > 1);
        //Split the sentences into the words
        JavaRDD<String> justWordsRDD = removedBlankLines.flatMap(sentences -> Arrays.asList(sentences.split(" ")).iterator());
        JavaRDD<String> removeBlankWordRDD = justWordsRDD.filter(word -> word.length() > 1);

        //Convert to pair RDD - Key value
        JavaPairRDD<String, Long> wordCountPairRDD = removeBlankWordRDD.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> total = wordCountPairRDD.reduceByKey((val1, val2) -> val1+val2);
        JavaPairRDD<Long, String> swicthedWordCountRDD = total.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));
        JavaPairRDD<Long, String> finalPairRDD = swicthedWordCountRDD.sortByKey(false);


        List<Tuple2<Long, String>> words = finalPairRDD.take(50);
        words.forEach(name -> System.out.println(name));
        sc.close();

    }
}

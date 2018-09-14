import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by chenbin@tieserv.com on 2018/9/13 0013.
 */
public class SparkWordCount {
    private static final String LOCAL_PATH = "C:\\Users\\Administrator\\Desktop\\profile";
    private static final String HDFS_PATH = "hdfs://master:8020/demo1/profile";
    private static final String LOCAL_MODEL = "local";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SPARK-WOED-COUNT");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(HDFS_PATH);
//        lines
//                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<>(word, 1))
//                .reduceByKey((a, b) -> a + b).foreach(word-> {
//                    System.err.println(word);
//                });//java8可以这么写也是很酷炫咯
        JavaRDD woeds = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD pairs = woeds.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(String word) throws Exception {
                return new Tuple2(word,1);
            }
        });
        JavaPairRDD wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.err.println(wordCount._1 + " : " + wordCount._2);
            }
        });
        sc.close();
//        sc.textFile("hdfs://master:8020/demo1/profile).flatMap(line->line.split(" ")).map((_,1)).reduceByKey(_+_).foreach(s->{println(s._1)})

    }
}

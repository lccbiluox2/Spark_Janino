/*
 * $SPARK_HOME/bin/spark-submit --class JavaJoin ~/Desktop/JavaScan-0.0.1-SNAPSHOT-jar-with-dependencies.jar  hdfs://localhost:8020/tmp/benchmark/text/tiny/rankings hdfs://localhost:8020/tmp/benchmark/text/tiny/uservisits 1980-01-01 1980-04-01
 * */
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class JavaJoin {
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("Java_Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> ranking = sc.textFile(args[0]);
		JavaRDD<String> uservisit = sc.textFile(args[1]);

		@SuppressWarnings("serial")
		JavaRDD<String[]> rankingTable = ranking
				.map(new Function<String, String[]>() {
					public String[] call(String s) {
						return s.split(",");
					}
				});

		JavaRDD<String[]> uservisitTable = uservisit
				.map(new Function<String, String[]>() {
					public String[] call(String s) {
						return s.split(",");
					}
				});
		final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		final Date start_date = format.parse(args[2]);
		final Date end_date = format.parse(args[3]);

		JavaRDD<String[]> visitRDD = uservisitTable
				.filter(new Function<String[], Boolean>() {
					public Boolean call(String[] x) throws ParseException {
						Date visitDate = format.parse(x[2]);
						return (visitDate.compareTo(start_date) >= 0)
								&& (visitDate.compareTo(end_date) <= 0);
					}
				});
		PairFunction<String[], String, String[]> rankingKeyData = new PairFunction<String[], String, String[]>() {
			public Tuple2<String, String[]> call(String[] x) {
				return new Tuple2(x[0], x);
			}
		};
		JavaPairRDD<String, String[]> rankingPairs = rankingTable
				.mapToPair(rankingKeyData);

		PairFunction<String[], String, String[]> visitKeyData = new PairFunction<String[], String, String[]>() {
			public Tuple2<String, String[]> call(String[] x) {
				return new Tuple2(x[1], x);
			}
		};
		JavaPairRDD<String, String[]> visitPairs = visitRDD
				.mapToPair(visitKeyData);

		JavaPairRDD<String, Tuple2<String[], String[]>> joinTable = visitPairs
				.join(rankingPairs);

		JavaRDD<Tuple2<String, Tuple2<Double, Double>>> sourceIPAsKey = joinTable
				.map(new Function<Tuple2<String, Tuple2<String[], String[]>>, Tuple2<String, Tuple2<Double, Double>>>() {
					public Tuple2<String, Tuple2<Double, Double>> call(
							Tuple2<String, Tuple2<String[], String[]>> t) {
						return new Tuple2(t._2._1[0], new Tuple2(Double
								.parseDouble(t._2._2[1]), Double
								.parseDouble(t._2._1[3])));
					}
				});
		JavaPairRDD<String, Tuple2<Double, Double>> IPKeyPairs = JavaPairRDD
				.fromJavaRDD(sourceIPAsKey);
		JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, Integer>> groupSourceIP = IPKeyPairs
				.mapValues(new Function<Tuple2<Double, Double>, Tuple2<Tuple2<Double, Double>, Integer>>() {
					public Tuple2<Tuple2<Double, Double>, Integer> call(
							Tuple2<Double, Double> t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2(t, 1);
					}
				});
		JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, Integer>> reduceByKey = groupSourceIP
				.reduceByKey(new Function2<Tuple2<Tuple2<Double, Double>, Integer>, Tuple2<Tuple2<Double, Double>, Integer>, Tuple2<Tuple2<Double, Double>, Integer>>() {
					public Tuple2<Tuple2<Double, Double>, Integer> call(
							Tuple2<Tuple2<Double, Double>, Integer> t1,
							Tuple2<Tuple2<Double, Double>, Integer> t2)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2(new Tuple2(t1._1._1 + t2._1._1,
								t1._1._2 + t2._1._2), t1._2 + t2._2);
					}
				});

		JavaRDD<Tuple2<Double, Tuple2<Double, String>>> reduceResult = reduceByKey
				.map(new Function<Tuple2<String, Tuple2<Tuple2<Double, Double>, Integer>>, Tuple2<Double, Tuple2<Double, String>>>() {
					public Tuple2<Double, Tuple2<Double, String>> call(
							Tuple2<String, Tuple2<Tuple2<Double, Double>, Integer>> t)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2(t._2._1._2, new Tuple2(t._2._1._1
								/ t._2._2, t._1));
					}
				});
		JavaPairRDD<Double, Tuple2<Double, String>> result = JavaPairRDD
				.fromJavaRDD(reduceResult);
		JavaPairRDD<Double, Tuple2<Double, String>> sortingResult = result
				.sortByKey(false);

		System.out.println("Here are some examples:");
		for (Tuple2<Double, Tuple2<Double, String>> line : sortingResult
				.take(1)) {
			System.out.println(line._2._2 + " " + line._1 + " " + line._2._1);
		}
	}
}

/*
 * $SPARK_HOME/bin/spark-submit --class <classname> <jar_file_path> <file_path> <column_index> <definition of the expression>
 * For example: 
 *            $SPARK_HOME/bin/spark-submit --class JavaAggregation ~/Desktop/JavaScan-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://localhost:8020/tmp/benchmark/text/tiny/uservisits -pn "x,y" -pt "double,double" "x + y"
 * mvn assembly:assembly  
 * */
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import scala.Tuple2;

public class JavaAggregation {
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("Java_Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0]);
		JavaPairRDD<String, Double> pairs = lines
				.mapToPair(new PairFunction<String, String, Double>() {
					@SuppressWarnings("unchecked")
					public Tuple2<String, Double> call(String x) {
						return new Tuple2(x.split(",")[0].substring(0, 9),
								Double.parseDouble(x.split(",")[3]));
					}
				});

		StringExpression exp = new StringExpression(Arrays.copyOfRange(args, 1,
				6));

		IExpressionEvaluator ee = exp.getExpression();
		Class[] parameterTypes = exp.getParameterType();
		JavaPairRDD<String, Double> resultRDD = pairs
				.reduceByKey(new ReduceFunction(ee, parameterTypes));

		System.out.println("Here are some examples:");
		for (Tuple2<String, Double> line : resultRDD.take(10)) {
			System.out.println(line._1 + " " + line._2);
		}

	}
}

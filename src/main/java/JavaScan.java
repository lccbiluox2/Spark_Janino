/*
 * $SPARK_HOME/bin/spark-submit --class <classname> <jar_file_path> <file_path> <column_index> <definition of the expression>
 * For example: 
 *            $SPARK_HOME/bin/spark-submit --class JavaScan ~/Desktop/JavaScan-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://localhost:8020/tmp/benchmark/text/tiny/rankings 1 -pn "x" -pt "int" "x > 50"
 * mvn assembly:assembly  
 * */

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public class JavaScan {
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("Java_Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0]);

		@SuppressWarnings("serial")
		JavaRDD<String[]> rows = lines.map(new Function<String, String[]>() {
			public String[] call(String s) {
				return s.split(",");
			}
		});
		StringExpression exp = new StringExpression(Arrays.copyOfRange(args, 2,
				7));

		IExpressionEvaluator ee = exp.getExpression();
		Class[] parameterTypes = exp.getParameterType();
		JavaRDD<String[]> resultRDD = rows.filter(new Conditions(ee,
				parameterTypes, Integer.parseInt(args[1])));

		System.out.println("Here are some examples:");
		for (String[] line : resultRDD.take(10)) {
			System.out.println(line[0] + " " + line[1] + " " + line[2]);
		}
	}
}

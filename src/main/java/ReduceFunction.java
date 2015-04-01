import org.apache.spark.api.java.function.Function2;
import org.codehaus.commons.compiler.IExpressionEvaluator;

@SuppressWarnings("serial")
class ReduceFunction implements Function2<Double, Double, Double> {
	public static IExpressionEvaluator ee;
	public static Class[] paraType;

	public ReduceFunction(IExpressionEvaluator ee, Class[] parameterTypes)
			throws Exception {
		ReduceFunction.ee = ee;
		ReduceFunction.paraType = parameterTypes;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Double call(Double x, Double y) throws Exception {
		IExpressionEvaluator exp = ReduceFunction.ee;
		Class[] type = ReduceFunction.paraType;
		int len = type.length; // length of the parameters 2
		Object[] arguments = new Object[len];
		arguments[0] = x;
		arguments[1] = y;

		return (Double) exp.evaluate(arguments);
	}
}
import org.apache.spark.api.java.function.Function;
import org.codehaus.commons.compiler.IExpressionEvaluator;

@SuppressWarnings("serial")
class Conditions implements Function<String[], Boolean> {
	public static Integer index;
	public static IExpressionEvaluator ee;
	public static Class paraType;

	public Conditions(IExpressionEvaluator ee, Class[] parameterTypes,
			Integer index) throws Exception {
		Conditions.ee = ee;
		Conditions.paraType = parameterTypes[0];
		Conditions.index = index;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Boolean call(String[] x) throws Exception {
		IExpressionEvaluator exp = Conditions.ee;
		Integer s = Conditions.index;
		Class type = Conditions.paraType;
		Object[] arguments = new Object[1];
		String value = x[s];
		arguments[0] = DemoBase.createObject(type, x[s]);

		return (Boolean) exp.evaluate(arguments);
	}
}
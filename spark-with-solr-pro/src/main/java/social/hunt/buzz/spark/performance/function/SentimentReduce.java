/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.spark.api.java.function.Function2;

/**
 * @author lewis
 *
 */
public class SentimentReduce implements Function2<Double, Double, Double>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8906661414607659632L;

	/**
	 * 
	 */
	public SentimentReduce() {
	}

	@Override
	public Double call(Double v1, Double v2) throws Exception {
		BigDecimal val = new BigDecimal(v1);
		val = val.add(new BigDecimal(v2));
		val = val.divide(new BigDecimal(2), 10, RoundingMode.HALF_UP);
		double sScore = val.doubleValue();
		if (sScore > 0) {
			sScore = Math.min(sScore, 1);
		} else if (sScore < 0) {
			sScore = Math.max(sScore, -1);
		}
		return sScore;
	}

}

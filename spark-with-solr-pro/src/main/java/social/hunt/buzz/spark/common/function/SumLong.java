/**
 * 
 */
package social.hunt.buzz.spark.common.function;

import org.apache.spark.api.java.function.Function2;

/**
 * @author lewis
 *
 */
public class SumLong implements Function2<Long, Long, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1872155411112187057L;

	@Override
	public Long call(Long l1, Long l2) throws Exception {
		return l1 + l2;
	}

}

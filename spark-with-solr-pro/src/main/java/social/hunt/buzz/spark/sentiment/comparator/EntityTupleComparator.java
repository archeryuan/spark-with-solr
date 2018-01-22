/**
 * 
 */
package social.hunt.buzz.spark.sentiment.comparator;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

/**
 * @author lewis
 *
 */
public class EntityTupleComparator implements Comparator<Tuple2<String, Long>>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5347240157892935264L;

	/**
	 * 
	 */
	public EntityTupleComparator() {
	}

	@Override
	public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
		return o1._2().compareTo(o2._2());
	}

}

/**
 * 
 */
package social.hunt.buzz.spark.sentiment.comparator;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.commons.lang3.tuple.Pair;

import scala.Tuple2;
import social.hunt.buzz.spark.sentiment.function.NameEntityCount.EntityType;

/**
 * @author lewis
 *
 */
public class EntityTuplePairComparator implements Comparator<Tuple2<Pair<EntityType, String>, Long>>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4712666432078328188L;

	/**
	 * 
	 */
	public EntityTuplePairComparator() {
	}

	@Override
	public int compare(Tuple2<Pair<EntityType, String>, Long> o1, Tuple2<Pair<EntityType, String>, Long> o2) {
		return o1._2().compareTo(o2._2());
	}

}

/**
 * 
 */
package social.hunt.buzz.spark.sentiment.function;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import social.hunt.buzz.spark.sentiment.function.NameEntityCount.EntityType;

/**
 * @author lewis
 *
 */
public class EntityTypeFilterFunction implements Function<Tuple2<Pair<EntityType, String>, Long>, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2338999627805062054L;
	private final EntityType entityType;

	/**
	 * @param entityType
	 */
	public EntityTypeFilterFunction(EntityType entityType) {
		super();
		this.entityType = entityType;
	}

	@Override
	public Boolean call(Tuple2<Pair<EntityType, String>, Long> obj) throws Exception {
		if (obj != null) {
			if (entityType.equals(obj._1().getLeft()))
				return true;
		}

		return false;
	}

}

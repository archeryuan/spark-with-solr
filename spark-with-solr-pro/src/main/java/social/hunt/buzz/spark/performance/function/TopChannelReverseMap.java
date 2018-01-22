/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;

import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.sa.common.definition.SolrFieldDefinition;

import scala.Tuple2;
import social.hunt.buzz.spark.data.TopChannelEntity;

/**
 * @author Archer Yuan
 *
 */
public class TopChannelReverseMap implements PairFunction<Tuple2<String, TopChannelEntity>, Long, TopChannelEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6656035546742832750L;

	/**
	 * 
	 */
	public TopChannelReverseMap() {
	}

	@Override
	public Tuple2<Long, TopChannelEntity> call(Tuple2<String, TopChannelEntity> tuple) throws Exception {
             return new Tuple2<Long, TopChannelEntity>(tuple._2.getEgagement(), tuple._2);
	}

}

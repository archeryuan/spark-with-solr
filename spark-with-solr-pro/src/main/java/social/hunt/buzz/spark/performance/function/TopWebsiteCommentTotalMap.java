package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import social.hunt.buzz.spark.data.TopWebsiteEntity;

public class TopWebsiteCommentTotalMap implements PairFunction<Tuple2<String, TopWebsiteEntity>, Long, TopWebsiteEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3778479205840134893L;

	@Override
	public Tuple2<Long, TopWebsiteEntity> call(Tuple2<String, TopWebsiteEntity> tuple) {
		return new Tuple2<Long, TopWebsiteEntity>(tuple._2.getCommentTotal(), tuple._2);
	}
}

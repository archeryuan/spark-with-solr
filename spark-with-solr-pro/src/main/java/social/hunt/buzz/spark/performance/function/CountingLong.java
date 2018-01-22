/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import social.hunt.buzz.spark.performance.definition.ResultType;

import com.sa.common.definition.SolrFieldDefinition;

/**
 * @author lewis
 *
 */
public class CountingLong implements PairFlatMapFunction<SolrDocument, ResultType, Long>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2592948180727348384L;

	public CountingLong() {
		super();
	}

	@Override
	public Iterable<Tuple2<ResultType, Long>> call(SolrDocument doc) throws Exception {
		ArrayList<Tuple2<ResultType, Long>> result = new ArrayList<Tuple2<ResultType, Long>>();

		if (doc != null) {
			/**
			 * Document Count
			 */
			result.add(new Tuple2<ResultType, Long>(ResultType.DOCUMENT_COUNT, 1L));

			/**
			 * Engagement Score
			 */
			MutableLong score = new MutableLong(0);

			if (doc.containsKey(SolrFieldDefinition.LIKE_COUNT.getName())) {
				score.add((long) doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName()));
			}

			if (doc.containsKey(SolrFieldDefinition.READ_COUNT.getName())) {
				score.add((long) doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName()));
			}

			if (doc.containsKey(SolrFieldDefinition.SHARE_COUNT.getName())) {
				score.add((long) doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName()));
			}

			if (doc.containsKey(SolrFieldDefinition.COMMENT_COUNT.getName())) {
				score.add((long) doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName()));
			}

			if (doc.containsKey(SolrFieldDefinition.DISLIKE_COUNT.getName())) {
				score.add((long) doc.getFieldValue(SolrFieldDefinition.DISLIKE_COUNT.getName()));
			}

			result.add(new Tuple2<ResultType, Long>(ResultType.ENGAGEMENT_SCORE, score.longValue()));

		}

		return result;
	}
}

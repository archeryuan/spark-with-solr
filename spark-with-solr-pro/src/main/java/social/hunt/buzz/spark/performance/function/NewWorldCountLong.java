/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
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
public class NewWorldCountLong extends FunctionUtils implements PairFlatMapFunction<SolrDocument, ResultType, Long>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7448857703325604916L;

	@Override
	public Iterable<Tuple2<ResultType, Long>> call(SolrDocument doc) throws Exception {
		ArrayList<Tuple2<ResultType, Long>> result = new ArrayList<Tuple2<ResultType, Long>>();

		if (doc != null) {

			String domain = extractDomainFromDoc(doc);
			if (StringUtils.endsWith(domain, ".tw") || StringUtils.endsWith(domain, ".cn") || "zhidao.baidu.com".equals(domain)) {
				result.add(new Tuple2<ResultType, Long>(ResultType.DOCUMENT_COUNT, 0L));
				result.add(new Tuple2<ResultType, Long>(ResultType.POSITIVE_COUNT, 0L));
				result.add(new Tuple2<ResultType, Long>(ResultType.NEGATIVE_COUNT, 0L));
				result.add(new Tuple2<ResultType, Long>(ResultType.ENGAGEMENT_SCORE, 0L));
				return result;
			}

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

			Object sentimentObj = doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
			if (null != sentimentObj) {
				try {
					Double sentimentScore = Double.parseDouble(sentimentObj.toString());
					if (sentimentScore >= 0.5) {
						result.add(new Tuple2<ResultType, Long>(ResultType.POSITIVE_COUNT, 1L));
						result.add(new Tuple2<ResultType, Long>(ResultType.NEGATIVE_COUNT, 0L));
					} else if (sentimentScore <= -0.5) {
						result.add(new Tuple2<ResultType, Long>(ResultType.POSITIVE_COUNT, 0L));
						result.add(new Tuple2<ResultType, Long>(ResultType.NEGATIVE_COUNT, 1L));
					} else {
						result.add(new Tuple2<ResultType, Long>(ResultType.POSITIVE_COUNT, 0L));
						result.add(new Tuple2<ResultType, Long>(ResultType.NEGATIVE_COUNT, 0L));
					}
				} catch (Exception e) {
					result.add(new Tuple2<ResultType, Long>(ResultType.POSITIVE_COUNT, 0L));
					result.add(new Tuple2<ResultType, Long>(ResultType.NEGATIVE_COUNT, 0L));
				}
			} else {
				result.add(new Tuple2<ResultType, Long>(ResultType.POSITIVE_COUNT, 0L));
				result.add(new Tuple2<ResultType, Long>(ResultType.NEGATIVE_COUNT, 0L));
			}
		}

		return result;
	}
}

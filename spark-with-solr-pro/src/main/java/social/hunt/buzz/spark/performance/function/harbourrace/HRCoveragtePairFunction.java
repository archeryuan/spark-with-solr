package social.hunt.buzz.spark.performance.function.harbourrace;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import social.hunt.buzz.spark.performance.function.FunctionUtils;

import com.sa.common.definition.SolrFieldDefinition;

public class HRCoveragtePairFunction extends FunctionUtils implements PairFunction<SolrDocument, String, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6033033101550572063L;

	public HRCoveragtePairFunction() {
	}

	@Override
	public Tuple2<String, Long> call(SolrDocument doc) throws Exception {
		String domain = extractDomainFromDoc(doc);
		if (StringUtils.endsWith(domain, ".tw") || StringUtils.endsWith(domain, ".cn") || "zhidao.baidu.com".equals(domain)) {
			return new Tuple2<String, Long>("", 0L);
		}

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

		return new Tuple2<String, Long>(domain, 1L + score.longValue());
	}
}

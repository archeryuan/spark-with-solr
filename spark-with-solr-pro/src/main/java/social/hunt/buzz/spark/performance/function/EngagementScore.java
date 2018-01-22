/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.Function;

import com.sa.common.definition.SolrFieldDefinition;

/**
 * @author lewis
 *
 */
public class EngagementScore implements Function<SolrDocument, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Long call(SolrDocument doc) throws Exception {
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

		return score.longValue();
	}

}

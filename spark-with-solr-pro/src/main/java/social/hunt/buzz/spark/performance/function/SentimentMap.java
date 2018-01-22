/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;

import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.Function;

import com.sa.common.definition.SolrFieldDefinition;

/**
 * @author lewis
 *
 */
public class SentimentMap extends FunctionUtils implements Function<SolrDocument, Double>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6656035546742832750L;

	/**
	 * 
	 */
	public SentimentMap() {
	}

	@Override
	public Double call(SolrDocument doc) throws Exception {

		Object sentimentObj = doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
		if (null != sentimentObj) {
			try {
				Double sentimentScore = Double.parseDouble(sentimentObj.toString());
				return sentimentScore;
			} catch (Exception e) {
			}
		}
		return new Double(0);
	}

}

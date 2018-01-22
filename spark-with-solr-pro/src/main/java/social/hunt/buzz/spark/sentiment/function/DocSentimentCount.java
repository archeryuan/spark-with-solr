/**
 * 
 */
package social.hunt.buzz.spark.sentiment.function;

import org.apache.commons.collections.ListUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import social.hunt.buzz.spark.sentiment.definition.SentimentDef;

import com.google.common.collect.Lists;
import com.sa.common.definition.SolrFieldDefinition;

/**
 * Things to calculate in this function:<BR>
 * 1) Daily sentiment score for each profile<BR>
 * 2) Daily sentiment(positive, negative, neutral) count for each profile<BR>
 * 3) Daily top 20 keywords & themes & its count for each profile's sentiment(positive, negative, neutral)<BR>
 * 4) Daily entity & its count for each profile<BR>
 * 
 * @author lewis
 *
 */
public class DocSentimentCount implements PairFlatMapFunction<SolrDocument, SentimentDef, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4600698936434378386L;

	// /**
	// * Number of positive document count
	// */
	// public static final String DOC_POSITIVE_COUNT = "pos";
	//
	// /**
	// * Number of negative document count
	// */
	// public static final String DOC_NEGATIVE_COUNT = "neg";
	//
	// /**
	// * Number of neutral document count
	// */
	// public static final String DOC_NEUTRAL_COUNT = "neu";

	/**
	 * 
	 */
	public DocSentimentCount() {
		Logger log = LoggerFactory.getLogger(DocSentimentCount.class);
		log.info("Calculating document count for each sentiment.");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterable<Tuple2<SentimentDef, Long>> call(SolrDocument doc) throws Exception {
		if (doc != null && doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
			Float sScoreFloat = (Float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
			if (sScoreFloat != null) {
				if (sScoreFloat > 0.22) {
					// It's a positive
					return Lists.newArrayList(new Tuple2<SentimentDef, Long>(SentimentDef.POSITIVE, 1L));
				} else if (sScoreFloat < -0.05) {
					// It's a negative
					return Lists.newArrayList(new Tuple2<SentimentDef, Long>(SentimentDef.NEGATIVE, 1L));
				} else {
					// It's a neutral
					return Lists.newArrayList(new Tuple2<SentimentDef, Long>(SentimentDef.NEUTRAL, 1L));
				}
			}
		}

		return ListUtils.EMPTY_LIST;
	}

}

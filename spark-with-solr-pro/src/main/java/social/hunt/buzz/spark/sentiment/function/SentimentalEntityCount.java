/**
 * 
 */
package social.hunt.buzz.spark.sentiment.function;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import social.hunt.buzz.spark.sentiment.definition.SentimentDef;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.StringUtils;

/**
 * @author lewis
 *
 */
public class SentimentalEntityCount implements PairFlatMapFunction<SolrDocument, String, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6185720979896251420L;

	private static final Logger log = LoggerFactory.getLogger(SentimentalEntityCount.class);

	public static final String TYPE_KEYWORD = "keyword";
	public static final String TYPE_THEME = "theme";

	private final String entityType;
	private final SentimentDef sentiment;

	/**
	 * @param entityType
	 * @param sentiment
	 */
	public SentimentalEntityCount(String entityType, SentimentDef sentiment) {
		super();
		this.entityType = entityType;
		this.sentiment = sentiment;

		log.info("Calculating sentimental entity count, entityType:{}, sentiment:{}", entityType, sentiment);
	}

	@Override
	public Iterable<Tuple2<String, Long>> call(SolrDocument doc) throws Exception {
		List<Tuple2<String, Long>> list = new ArrayList<Tuple2<String, Long>>();
		Collection<Object> coll = null;

		if (TYPE_KEYWORD.equals(getEntityType())) {
			switch (getSentiment()) {
			case NEGATIVE:
				if (doc.containsKey(SolrFieldDefinition.NEG_KEYWORDS.getName())) {
					coll = doc.getFieldValues(SolrFieldDefinition.NEG_KEYWORDS.getName());
					coll = removeSymbol(coll);
				}
				break;
			case NEUTRAL:
				if (doc.containsKey(SolrFieldDefinition.NEUTRAL_KEYWORDS.getName())) {
					coll = doc.getFieldValues(SolrFieldDefinition.NEUTRAL_KEYWORDS.getName());
					coll = removeSymbol(coll);
				}
				break;
			case POSITIVE:
				if (doc.containsKey(SolrFieldDefinition.POS_KEYWORDS.getName())) {
					coll = doc.getFieldValues(SolrFieldDefinition.POS_KEYWORDS.getName());
					coll = removeSymbol(coll);
				}
				break;
			}
		} else if (TYPE_THEME.equals(getEntityType())) {
			switch (getSentiment()) {
			case NEGATIVE:
				if (doc.containsKey(SolrFieldDefinition.NEG_THEMES.getName())) {
					coll = doc.getFieldValues(SolrFieldDefinition.NEG_THEMES.getName());
					coll = removeSymbol(coll);
				}
				break;
			case NEUTRAL:
				if (doc.containsKey(SolrFieldDefinition.NEUTRAL_THEMES.getName())) {
					coll = doc.getFieldValues(SolrFieldDefinition.NEUTRAL_THEMES.getName());
					coll = removeSymbol(coll);
				}
				break;
			case POSITIVE:
				if (doc.containsKey(SolrFieldDefinition.POS_THEMES.getName())) {
					coll = doc.getFieldValues(SolrFieldDefinition.POS_THEMES.getName());
					coll = removeSymbol(coll);
				}
				break;
			}
		}

		StringBuilder text = new StringBuilder();
		if (doc.containsKey(SolrFieldDefinition.TITLE.getName())) {
			text.append(StringUtils.defaultString((String) doc.getFieldValue(SolrFieldDefinition.TITLE.getName())));
		}
		if (doc.containsKey(SolrFieldDefinition.CONTENT.getName())) {
			if (doc.getFieldValue(SolrFieldDefinition.CONTENT.getName()) instanceof Collection) {
				Collection<String> contents = (Collection<String>) doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
				if (contents != null && !contents.isEmpty())
					text.append(StringUtils.join(contents));
			} else {
				text.append(StringUtils.defaultString((String) doc.getFieldValue(SolrFieldDefinition.CONTENT.getName())));
			}
		}

		if (StringUtils.isChinese(text.toString()) && text.length() > 1000) {
			text.replace(1000, text.length(), "");
		}

		if (coll != null && !coll.isEmpty()) {
			for (Object o : coll) {
				String str = (String) o;

				long occurence = 1;
				occurence = Math.max(occurence, StringUtils.countMatches(text, str));

				list.add(new Tuple2<String, Long>(str, occurence));
			}
		}

		return list;
	}

	private Collection<Object> removeSymbol(Collection<Object> coll) {
		List<Object> outColl = new ArrayList<Object>();

		if (coll != null) {
			next: for (Object o : coll) {
				String s = (String) o;
				if (StringUtils.containsEmoji(s))
					continue next;

				outColl.add(s);
			}
		}

		return outColl;
	}

	/**
	 * @return the entityType
	 */
	protected String getEntityType() {
		return entityType;
	}

	/**
	 * @return the sentiment
	 */
	protected SentimentDef getSentiment() {
		return sentiment;
	}

}

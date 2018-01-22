/**
 * 
 */
package social.hunt.buzz.spark.sentiment.function;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import social.hunt.buzz.spark.sentiment.function.NameEntityCount.EntityType;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.StringUtils;

/**
 * @author lewis
 *
 */
public class NameEntityCount implements PairFlatMapFunction<SolrDocument, Pair<EntityType, String>, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7424055475021852885L;

	public enum EntityType {
		PATTERN, COMPANY, PERSON, PLACE, PRODUCT, JOB_TITLE;
	}

	/**
	 * 
	 */
	public NameEntityCount() {
		Logger log = LoggerFactory.getLogger(NameEntityCount.class);
		log.info("Calculating entity count.");
	}

	@Override
	public Iterable<Tuple2<Pair<EntityType, String>, Long>> call(SolrDocument doc) throws Exception {
		List<Tuple2<Pair<EntityType, String>, Long>> list = new ArrayList<Tuple2<Pair<EntityType, String>, Long>>();

		if (doc != null) {
			next: for (EntityType eType : EntityType.values()) {
				Collection<Object> coll = null;

				switch (eType) {
				case COMPANY:
					if (doc.containsKey(SolrFieldDefinition.COMPANIES.getName())) {
						coll = doc.getFieldValues(SolrFieldDefinition.COMPANIES.getName());
						coll = removeSingleCharacter(coll);
						coll = removeSymbol(coll);
					}
					break;
				case JOB_TITLE:
					if (doc.containsKey(SolrFieldDefinition.JOB_TITLES.getName())) {
						coll = doc.getFieldValues(SolrFieldDefinition.JOB_TITLES.getName());
						coll = removeSingleCharacter(coll);
						coll = removeSymbol(coll);
					}
					break;
				case PATTERN:
					if (doc.containsKey(SolrFieldDefinition.PATTERNS.getName())) {
						coll = doc.getFieldValues(SolrFieldDefinition.PATTERNS.getName());
						coll = removeSingleCharacter(coll);
						coll = removeSymbol(coll);
					}
					break;
				case PERSON:
					if (doc.containsKey(SolrFieldDefinition.PERSONS.getName())) {
						coll = doc.getFieldValues(SolrFieldDefinition.PERSONS.getName());
						coll = removeSymbol(coll);
					}
					break;
				case PLACE:
					if (doc.containsKey(SolrFieldDefinition.PLACES.getName())) {
						coll = doc.getFieldValues(SolrFieldDefinition.PLACES.getName());
						coll = removeSymbol(coll);
					}
					break;
				case PRODUCT:
					if (doc.containsKey(SolrFieldDefinition.PRODUCTS.getName())) {
						coll = doc.getFieldValues(SolrFieldDefinition.PRODUCTS.getName());
						coll = removeSingleCharacter(coll);
						coll = removeSymbol(coll);
					}
					break;
				default:
					continue next;
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

						list.add(new Tuple2<Pair<EntityType, String>, Long>(Pair.of(eType, str), occurence));
					}
				}
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

	private Collection<Object> removeSingleCharacter(Collection<Object> coll) {
		List<Object> outColl = new ArrayList<Object>();

		if (coll != null) {
			for (Object o : coll) {
				String s = (String) o;
				if (StringUtils.length(s) == 1 && s.charAt(0) >= 32 && s.charAt(0) <= 126) {
					// do nothing
				} else {
					outColl.add(s);
				}
			}
		}

		return outColl;
	}
}

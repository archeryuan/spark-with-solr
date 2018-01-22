package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFunction;

import com.sa.common.definition.SolrFieldDefinition;

import scala.Tuple2;
import social.hunt.buzz.spark.data.SOVEntity;

public class SovMap implements PairFunction<SolrDocument, String, SOVEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2940043413734914500L;

	@Override
	public Tuple2<String, SOVEntity> call(SolrDocument doc) throws Exception {
		Long score = new Long(0);
		String reportDate = null;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		if (doc.containsKey(SolrFieldDefinition.PUBLISH_DATE.getName())) {
			Date pDate = (Date) doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
			if (pDate != null) {
				Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				c.setTime(pDate);
				c.set(Calendar.HOUR_OF_DAY, 0);
				c.set(Calendar.MINUTE, 0);
				c.set(Calendar.SECOND, 0);
				c.set(Calendar.MILLISECOND, 0);
				pDate = c.getTime();

				reportDate = df.format(pDate);
			}
		}

		SOVEntity entity = new SOVEntity();

		if (doc.containsKey(SolrFieldDefinition.LIKE_COUNT.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
		}

		// if (doc.containsKey(SolrFieldDefinition.READ_COUNT.getName())) {
		// score += (long) doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
		// }

		if (doc.containsKey(SolrFieldDefinition.SHARE_COUNT.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
		}

		if (doc.containsKey(SolrFieldDefinition.COMMENT_COUNT.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
		}

		/**
		 * Reaction
		 */
		if (doc.containsKey(SolrFieldDefinition.FB_ANGRY.getName())) {
			score += (Long) doc.getFieldValue(SolrFieldDefinition.FB_ANGRY.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_LOVE.getName())) {
			score += (Long) doc.getFieldValue(SolrFieldDefinition.FB_LOVE.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_SAD.getName())) {
			score += (Long) doc.getFieldValue(SolrFieldDefinition.FB_SAD.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_HAHA.getName())) {
			score += (Long) doc.getFieldValue(SolrFieldDefinition.FB_HAHA.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_THANKFUL.getName())) {
			score += (Long) doc.getFieldValue(SolrFieldDefinition.FB_THANKFUL.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_WOW.getName())) {
			score += (Long) doc.getFieldValue(SolrFieldDefinition.FB_WOW.getName());
		}
		entity.setEngagement(score);
		entity.setVolume(1L);
		entity.setDateStr(reportDate);

		return new Tuple2<String, SOVEntity>(reportDate, entity);
	}
}

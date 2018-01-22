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
import social.hunt.buzz.spark.data.MediaDistributionEntity;
import social.hunt.common.definition.Sns;

public class MediaDistributionMap implements PairFunction<SolrDocument, String, MediaDistributionEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8715948013748094299L;

	@Override
	public Tuple2<String, MediaDistributionEntity> call(SolrDocument doc) throws Exception {
		MediaDistributionEntity entity = new MediaDistributionEntity();

		String reportDate = null;

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

				entity.setDate(pDate);

				SimpleDateFormat df = new SimpleDateFormat("MMddyyyy");
				reportDate = df.format(pDate);
			}
		}

		if (doc.containsKey(SolrFieldDefinition.SNS_TYPE.getName())) {
			Object fieldValue = doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName());
			Sns sns = Sns.getSns((int) fieldValue);
			switch (sns) {
			case FACEBOOK:
				entity.setFbNum(1);
				break;
			case INSTAGRAM:
				entity.setIgNum(1);
				break;
			case WEIBO:
				entity.setWebNum(1);
				break;
			case TWITTER:
				entity.setTwitterNum(1);
				break;
			case YOUTUBE:
				entity.setYoutubeNum(1);
				break;
			case TELEGRAM:
				entity.setTelegramNum(1);
				break;
			default:
				break;
			}
		} else {
			entity.setWebNum(1);
		}

		return new Tuple2<String, MediaDistributionEntity>(reportDate, entity);

	}
}

package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import social.hunt.buzz.spark.data.NewTopChannel;
import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.common.definition.Sns;

import com.sa.common.definition.SolrFieldDefinition;

public class NewTopChangelMap implements PairFunction<SolrDocument, String, NewTopChannel>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3778479205840134893L;

	@Override
	public Tuple2<String, NewTopChannel> call(SolrDocument doc) {
		String uName = null;
		NewTopChannel entity = new NewTopChannel();

		if (doc != null) {
			long readCnt = 0l;
			long likeCnt = 0l;
			long commentCnt = 0l;
			long shareCnt = 0l;
			long negTheme = 0l;
			long posTheme = 0l;
			long neuTheme = 0l;

			String uId = null;
			String uPageLink = null;

			List<String> posThemes = null;
			List<String> negThemes = null;
			List<String> neuThemes = null;

			String channel = "NA";
			String media = "NA";

			float sScore = 0;

			if (doc.containsKey(SolrFieldDefinition.USER_NAME.getName()) && doc.containsKey(SolrFieldDefinition.SNS_TYPE.getName())) {
				uName = (int) doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName()) + "_"
						+ (String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName());
				channel = (String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName());
				media = Sns.getSns((int) doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName())).getNameEn();
			}

			// if (doc.containsKey(SolrFieldDefinition.READ_COUNT.getName())) {
			// readCnt = (long) doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
			// }
			if (doc.containsKey(SolrFieldDefinition.LIKE_COUNT.getName())) {
				likeCnt = (long) doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.COMMENT_COUNT.getName())) {
				commentCnt = (long) doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.SHARE_COUNT.getName())) {
				shareCnt = (long) doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.USER_ID.getName())) {
				uId = (String) doc.getFieldValue(SolrFieldDefinition.USER_ID.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.USER_PAGE_LINK.getName())) {
				uPageLink = (String) doc.getFieldValue(SolrFieldDefinition.USER_PAGE_LINK.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.POS_THEMES.getName())) {
				posThemes = (List<String>) doc.getFieldValue(SolrFieldDefinition.POS_THEMES.getName());
				posTheme = posThemes.size();
				if (posTheme > 0) {
					String temp = posThemes.get(0);
					for (int i = 1; i < posTheme; i++) {
						temp = temp + "/" + posThemes.get(i);
						if (i > 4) {
							break;
						}
					}

					entity.setPosthemes(temp);
				}
			}
			if (doc.containsKey(SolrFieldDefinition.NEG_THEMES.getName())) {
				negThemes = (List<String>) doc.getFieldValue(SolrFieldDefinition.NEG_THEMES.getName());
				negTheme = negThemes.size();
				if (negTheme > 0) {
					String temp = negThemes.get(0);
					for (int i = 1; i < negTheme; i++) {
						temp = temp + "/" + negThemes.get(i);
						if (i > 4) {
							break;
						}
					}

					entity.setNegthemes(temp);
				}
			}
			if (doc.containsKey(SolrFieldDefinition.NEUTRAL_THEMES.getName())) {
				neuThemes = (List<String>) doc.getFieldValue(SolrFieldDefinition.NEUTRAL_THEMES.getName());
				neuTheme = neuThemes.size();
				if (neuTheme > 0) {
					String temp = neuThemes.get(0);
					for (int i = 1; i < neuTheme; i++) {
						temp = temp + "/" + neuThemes.get(i);
						if (i > 4) {
							break;
						}
					}

					entity.setNeuthemes(temp);
				}
			}

			if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
				sScore = (float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
			}

			if (sScore < -0.45) {
				entity.setEngageNeg(likeCnt + commentCnt + shareCnt + 1l);
				entity.setPostNumNeg(1l);
				entity.setCommentNeg(commentCnt);
			} else if (sScore > 0.5) {
				entity.setEngagePos(likeCnt + commentCnt + shareCnt + 1l);
				entity.setPostNumPos(1l);
				entity.setCommentPos(commentCnt);
			} else {
				entity.setEngageNeu(likeCnt + commentCnt + shareCnt + 1l);
				entity.setPostNumNeu(1l);
				entity.setPostNumNeu(commentCnt);
			}
			
			entity.setCommentTotal(commentCnt);
			entity.setPostNumTotal(1l);
			entity.setEngageTotal(likeCnt + commentCnt + shareCnt + 1l);
			entity.setChannel(channel);
			entity.setMedia(media);
			entity.setuId(uId);

			if (uPageLink == null && uId != null) {
				if (media.equalsIgnoreCase("instagram")) {
					uPageLink = "https://www.instagram.com/" + channel;
				} else if (media.equalsIgnoreCase("facebook")) {
					uPageLink = "https://www.facebook.com/" + uId;
				} else if (media.equalsIgnoreCase("weibo")) {
					uPageLink = "www.weibo.com/u/" + uId;
				} else if (media.equalsIgnoreCase("youtube")) {
					uPageLink = "https://www.youtube.com/channel/" + uId;
				} else if (media.equalsIgnoreCase("twitter")) {
					uPageLink = "https://twitter.com/" + uId + "/status/" + uId;
				}

			}

			entity.setChannelUrl(uPageLink);

		}
		return new Tuple2<String, NewTopChannel>(uName, entity);
	}

}

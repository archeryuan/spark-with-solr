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
import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.buzz.spark.data.TopWebsiteEntity;
import social.hunt.buzz.spark.data.TrendEntity;

import com.sa.common.definition.SolrFieldDefinition;

public class TrendMap implements PairFunction<SolrDocument, String, TrendEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3778479205840134893L;

	@Override
	public Tuple2<String, TrendEntity> call(SolrDocument doc) {
		String dateStr = null;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		TrendEntity entity = new TrendEntity();

		if (doc != null) {
			long likeCnt = 0l;
			long commentCnt = 0l;
			long shareCnt = 0l;
			float sScore = 0;
			int snsType = 0;
			String domain = null;
			String people = null;

			if (doc.containsKey(SolrFieldDefinition.PUBLISH_DATE.getName())) {
				Date pubDate = (Date) doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				dateStr = df.format(pubDate);
			}

			// if (doc.containsKey(SolrFieldDefinition.READ_COUNT.getName())){
			// readCnt = (long)doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
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

			if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
				sScore = (float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
				sScore = (float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.SNS_TYPE.getName())) {
				snsType = (int) doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.DOMAIN.getName())) {
				domain = (String) doc.getFieldValue(SolrFieldDefinition.DOMAIN.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.USER_NAME.getName())) {
				people = (String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName());
			}

			if (doc.containsKey(SolrFieldDefinition.AUTHOR.getName())) {
				people = (String) doc.getFieldValue(SolrFieldDefinition.AUTHOR.getName());
			}

			long fbAngry = 0, fbLove = 0, fbSad = 0, fbHaha = 0, fbThankful = 0, fbWow = 0;
			/**
			 * Reaction
			 */
			if (doc.containsKey(SolrFieldDefinition.FB_ANGRY.getName())) {
				fbAngry += (Long) doc.getFieldValue(SolrFieldDefinition.FB_ANGRY.getName());
			}
			if (doc.containsKey(SolrFieldDefinition.FB_LOVE.getName())) {
				fbLove += (Long) doc.getFieldValue(SolrFieldDefinition.FB_LOVE.getName());
			}
			if (doc.containsKey(SolrFieldDefinition.FB_SAD.getName())) {
				fbSad += (Long) doc.getFieldValue(SolrFieldDefinition.FB_SAD.getName());
			}
			if (doc.containsKey(SolrFieldDefinition.FB_HAHA.getName())) {
				fbHaha += (Long) doc.getFieldValue(SolrFieldDefinition.FB_HAHA.getName());
			}
			if (doc.containsKey(SolrFieldDefinition.FB_THANKFUL.getName())) {
				fbThankful += (Long) doc.getFieldValue(SolrFieldDefinition.FB_THANKFUL.getName());
			}
			if (doc.containsKey(SolrFieldDefinition.FB_WOW.getName())) {
				fbWow += (Long) doc.getFieldValue(SolrFieldDefinition.FB_WOW.getName());
			}

			if (sScore < -0.45) {
				if (snsType == 0) {
					entity.setWebNegPost(1l);
					entity.setWebNegEngage(likeCnt + commentCnt + shareCnt);
					entity.setWebTotalEngage(likeCnt + commentCnt + shareCnt);
					entity.setWebTotalPost(1l);
					entity.setWebTotalComments(commentCnt);
					entity.setWebNegComments(commentCnt);
				} else {
					if (snsType == 1) {
						entity.setFacebookNegPost(1l);
						entity.setFacebookNegEngage(likeCnt + commentCnt + shareCnt + fbHaha + fbAngry + fbLove + fbSad + fbThankful
								+ fbWow);
						entity.setFacebookTotalEngage(likeCnt + commentCnt + shareCnt + fbHaha + fbAngry + fbLove + fbSad + fbThankful
								+ fbWow);
						entity.setFacebookTotalPost(1l);
						entity.setFacebookTotalComments(commentCnt);
						entity.setFacebookNegComments(commentCnt);
					} else if (snsType == 5) {
						entity.setTwitterNegPost(1l);
						entity.setTwitterNegEngage(likeCnt + commentCnt + shareCnt);
						entity.setTwitterTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setTwitterTotalPost(1l);
						entity.setTwitterTotalComments(commentCnt);
						entity.setTwitterNegComments(commentCnt);
					} else if (snsType == 3) {
						entity.setWeiboNegPost(1l);
						entity.setWeiboNegEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeiboTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeiboTotalPost(1l);
						entity.setWeiboTotalComments(commentCnt);
						entity.setWeiboNegComments(commentCnt);
					} else if (snsType == 9) {
						entity.setYoutubeNegPost(1l);
						entity.setYoutubeNegEngage(likeCnt + commentCnt + shareCnt);
						entity.setYoutubeTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setYoutubeTotalPost(1l);
						entity.setYoutubeTotalComments(commentCnt);
						entity.setYoutubeNegComments(commentCnt);
					} else if (snsType == 2) {
						entity.setInstagramNegPost(1l);
						entity.setInstagramNegEngage(likeCnt + commentCnt + shareCnt);
						entity.setInstagramTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setInstagramTotalPost(1l);
						entity.setInstagramTotalComments(commentCnt);
						entity.setInstagramNegComments(commentCnt);
					} else if (snsType == 22) {
						entity.setWeixinNegPost(1l);
						entity.setWeixinNegEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeixinTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeixinTotalPost(1l);
						entity.setWeixinNegComments(commentCnt);
					} else if (snsType == 100) {
						entity.setPlurkNegPost(1l);
						entity.setPlurkNegEngage(likeCnt + commentCnt + shareCnt);
						entity.setPlurkTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setPlurkTotalPost(1l);
						entity.setPlurkTotalComments(commentCnt);
						entity.setPlurkNegComments(commentCnt);
					} else if (snsType == 101) {
						entity.setLineqNegPost(1l);
						entity.setLineqNegEngage(likeCnt + commentCnt + shareCnt);
						entity.setLineqTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setLineqTotalPost(1l);
						entity.setLineqTotalComments(commentCnt);
						entity.setLineqNegComments(commentCnt);
					}
				}

				if (domain != null) {
					entity.getDomainNeg().add(domain);
				}

				if (people != null) {
					entity.getPeopleNeg().add(people);
				}

			} else if (sScore > 0.5) {
				if (snsType == 0) {
					entity.setWebPosPost(1l);
					entity.setWebPosEngage(likeCnt + commentCnt + shareCnt);
					entity.setWebTotalEngage(likeCnt + commentCnt + shareCnt);
					entity.setWebTotalPost(1l);
					entity.setWebTotalComments(commentCnt);
					entity.setWebPosComments(commentCnt);
				} else {
					if (snsType == 1) {
						entity.setFacebookPosPost(1l);
						entity.setFacebookPosEngage(likeCnt + commentCnt + shareCnt + fbHaha + fbAngry + fbLove + fbSad + fbThankful
								+ fbWow);
						entity.setFacebookTotalEngage(likeCnt + commentCnt + shareCnt + fbHaha + fbAngry + fbLove + fbSad + fbThankful
								+ fbWow);
						entity.setFacebookTotalPost(1l);
						entity.setFacebookTotalComments(commentCnt);
						entity.setFacebookPosComments(commentCnt);
					} else if (snsType == 5) {
						entity.setTwitterPosPost(1l);
						entity.setTwitterPosEngage(likeCnt + commentCnt + shareCnt);
						entity.setTwitterTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setTwitterTotalPost(1l);
						entity.setTwitterTotalComments(commentCnt);
						entity.setTwitterPosComments(commentCnt);
					} else if (snsType == 3) {
						entity.setWeiboPosPost(1l);
						entity.setWeiboPosEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeiboTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeiboTotalPost(1l);
						entity.setWeiboTotalComments(commentCnt);
						entity.setWeiboPosComments(commentCnt);
					} else if (snsType == 9) {
						entity.setYoutubePosPost(1l);
						entity.setYoutubePosEngage(likeCnt + commentCnt + shareCnt);
						entity.setYoutubeTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setYoutubeTotalPost(1l);
						entity.setYoutubeTotalComments(commentCnt);
						entity.setYoutubePosComments(commentCnt);
					} else if (snsType == 2) {
						entity.setInstagramPosPost(1l);
						entity.setInstagramPosEngage(likeCnt + commentCnt + shareCnt);
						entity.setInstagramTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setInstagramTotalPost(1l);
						entity.setInstagramTotalComments(commentCnt);
						entity.setInstagramPosComments(commentCnt);
					} else if (snsType == 22) {
						entity.setWeixinPosPost(1l);
						entity.setWeixinPosEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeixinTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeixinTotalPost(1l);
						entity.setWeixinTotalComments(commentCnt);
						entity.setWeixinPosComments(commentCnt);
					} else if (snsType == 100) {
						entity.setPlurkPosPost(1l);
						entity.setPlurkPosEngage(likeCnt + commentCnt + shareCnt);
						entity.setPlurkTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setPlurkTotalPost(1l);
						entity.setPlurkTotalComments(commentCnt);
						entity.setPlurkPosComments(commentCnt);
					} else if (snsType == 101) {
						entity.setLineqPosPost(1l);
						entity.setLineqPosEngage(likeCnt + commentCnt + shareCnt);
						entity.setLineqTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setLineqTotalPost(1l);
						entity.setLineqTotalComments(commentCnt);
						entity.setLineqPosComments(commentCnt);
					}
				}

				if (domain != null) {
					entity.getDomainPos().add(domain);
				}

				if (people != null) {
					entity.getPeoplePos().add(people);
				}
			} else {
				if (snsType == 0) {
					entity.setWebNeuPost(1l);
					entity.setWebNeuEngage(likeCnt + commentCnt + shareCnt);
					entity.setWebTotalEngage(likeCnt + commentCnt + shareCnt);
					entity.setWebTotalPost(1l);
					entity.setWebTotalComments(commentCnt);
					entity.setWebNeuComments(commentCnt);
				} else {
					if (snsType == 1) {
						entity.setFacebookNeuPost(1l);
						entity.setFacebookNeuEngage(likeCnt + commentCnt + shareCnt + fbHaha + fbAngry + fbLove + fbSad + fbThankful
								+ fbWow);
						entity.setFacebookTotalEngage(likeCnt + commentCnt + shareCnt + fbHaha + fbAngry + fbLove + fbSad + fbThankful
								+ fbWow);
						entity.setFacebookTotalPost(1l);
						entity.setFacebookTotalComments(commentCnt);
						entity.setFacebookNeuComments(commentCnt);
					} else if (snsType == 5) {
						entity.setTwitterNeuPost(1l);
						entity.setTwitterNeuEngage(likeCnt + commentCnt + shareCnt);
						entity.setTwitterTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setTwitterTotalPost(1l);
						entity.setTwitterTotalComments(commentCnt);
						entity.setTwitterNeuComments(commentCnt);
					} else if (snsType == 3) {
						entity.setWeiboNeuPost(1l);
						entity.setWeiboNeuEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeiboTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeiboTotalPost(1l);
						entity.setWeiboTotalComments(commentCnt);
						entity.setWeiboNeuComments(commentCnt);
					} else if (snsType == 9) {
						entity.setYoutubeNeuPost(1l);
						entity.setYoutubeNeuEngage(likeCnt + commentCnt + shareCnt);
						entity.setYoutubeTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setYoutubeTotalPost(1l);
						entity.setYoutubeTotalComments(commentCnt);
						entity.setYoutubeNeuComments(commentCnt);
					} else if (snsType == 2) {
						entity.setInstagramNeuPost(1l);
						entity.setInstagramNeuEngage(likeCnt + commentCnt + shareCnt);
						entity.setInstagramTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setInstagramTotalPost(1l);
						entity.setInstagramTotalComments(commentCnt);
						entity.setInstagramNeuComments(commentCnt);
					} else if (snsType == 22) {
						entity.setWeixinNeuPost(1l);
						entity.setWeixinNeuEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeixinTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setWeixinTotalPost(1l);
						entity.setWeixinTotalComments(commentCnt);
						entity.setWeixinNeuComments(commentCnt);
					} else if (snsType == 100) {
						entity.setPlurkNeuPost(1l);
						entity.setPlurkNeuEngage(likeCnt + commentCnt + shareCnt);
						entity.setPlurkTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setPlurkTotalPost(1l);
						entity.setPlurkTotalComments(commentCnt);
						entity.setPlurkNeuComments(commentCnt);
					} else if (snsType == 101) {
						entity.setLineqNeuPost(1l);
						entity.setLineqNeuEngage(likeCnt + commentCnt + shareCnt);
						entity.setLineqTotalEngage(likeCnt + commentCnt + shareCnt);
						entity.setLineqTotalPost(1l);
						entity.setLineqTotalComments(commentCnt);
						entity.setLineqNeuComments(commentCnt);
					}
				}

				if (domain != null) {
					entity.getDomainNeu().add(domain);
				}

				if (people != null) {
					entity.getPeopleNeu().add(people);
				}
			}

			if (domain != null) {
				entity.getDomain().add(domain);
			}

			if (people != null) {
				entity.getPeople().add(people);
			}
			entity.setDateStr(dateStr);
		}
		return new Tuple2<String, TrendEntity>(dateStr, entity);
	}

}

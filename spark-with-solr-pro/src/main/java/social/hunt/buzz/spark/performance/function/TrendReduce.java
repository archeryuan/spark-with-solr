/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.spark.api.java.function.Function2;

import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.buzz.spark.data.TrendEntity;

/**
 * @author Archer Yuan
 *
 */
public class TrendReduce implements Function2<TrendEntity, TrendEntity, TrendEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8906661414607659632L;

	/**
	 * 
	 */
	public TrendReduce() {
	}

	@Override
	public TrendEntity call(TrendEntity v1, TrendEntity v2) throws Exception {
		TrendEntity entity = v1;

		entity.setWebNegEngage(v1.getWebNegEngage() + v2.getWebNegEngage());
		entity.setWebNegPost(v1.getWebNegPost() + v2.getWebNegPost());
		entity.setWebNeuEngage(v1.getWebNeuEngage() + v2.getWebNeuEngage());
		entity.setWebNeuPost(v1.getWebNeuPost() + v2.getWebNeuPost());
		entity.setWebPosEngage(v1.getWebPosEngage() + v2.getWebPosEngage());
		entity.setWebPosPost(v1.getWebPosPost() + v2.getWebPosPost());
		entity.setWebTotalEngage(v1.getWebTotalEngage() + v2.getWebTotalEngage());
		entity.setWebTotalPost(v1.getWebTotalPost() + v2.getWebTotalPost());
		entity.setWebNegComments(v1.getWebNegComments() + v2.getWebNegComments());
		entity.setWebPosComments(v1.getWebPosComments() + v2.getWebPosComments());
		entity.setWebNeuComments(v1.getWebNeuComments() + v2.getWebNeuComments());
		entity.setWebTotalComments(v1.getWebTotalComments() + v2.getWebTotalComments());

		entity.setFacebookNegEngage(v1.getFacebookNegEngage() + v2.getFacebookNegEngage());
		entity.setFacebookNegPost(v1.getFacebookNegPost() + v2.getFacebookNegPost());
		entity.setFacebookNeuEngage(v1.getFacebookNeuEngage() + v2.getFacebookNeuEngage());
		entity.setFacebookNeuPost(v1.getFacebookNeuPost() + v2.getFacebookNeuPost());
		entity.setFacebookPosEngage(v1.getFacebookPosEngage() + v2.getFacebookPosEngage());
		entity.setFacebookPosPost(v1.getFacebookPosPost() + v2.getFacebookPosPost());
		entity.setFacebookTotalEngage(v1.getFacebookTotalEngage() + v2.getFacebookTotalEngage());
		entity.setFacebookTotalPost(v1.getFacebookTotalPost() + v2.getFacebookTotalPost());
		entity.setFacebookNegComments(v1.getFacebookNegComments() + v2.getFacebookNegComments());
		entity.setFacebookPosComments(v1.getFacebookPosComments() + v2.getFacebookPosComments());
		entity.setFacebookNeuComments(v1.getFacebookNeuComments() + v2.getFacebookNeuComments());
		entity.setFacebookTotalComments(v1.getFacebookTotalComments() + v2.getFacebookTotalComments());

		entity.setTwitterNegEngage(v1.getTwitterNegEngage() + v2.getTwitterNegEngage());
		entity.setTwitterNegPost(v1.getTwitterNegPost() + v2.getTwitterNegPost());
		entity.setTwitterNeuEngage(v1.getTwitterNeuEngage() + v2.getTwitterNeuEngage());
		entity.setTwitterNeuPost(v1.getTwitterNeuPost() + v2.getTwitterNeuPost());
		entity.setTwitterPosEngage(v1.getTwitterPosEngage() + v2.getTwitterPosEngage());
		entity.setTwitterPosPost(v1.getTwitterPosPost() + v2.getTwitterPosPost());
		entity.setTwitterTotalEngage(v1.getTwitterTotalEngage() + v2.getTwitterTotalEngage());
		entity.setTwitterTotalPost(v1.getTwitterTotalPost() + v2.getTwitterTotalPost());
		entity.setTwitterNegComments(v1.getTwitterNegComments() + v2.getTwitterNegComments());
		entity.setTwitterPosComments(v1.getTwitterPosComments() + v2.getTwitterPosComments());
		entity.setTwitterNeuComments(v1.getTwitterNeuComments() + v2.getTwitterNeuComments());
		entity.setTwitterTotalComments(v1.getTwitterTotalComments() + v2.getTwitterTotalComments());

		entity.setWeiboNegEngage(v1.getWeiboNegEngage() + v2.getWeiboNegEngage());
		entity.setWeiboNegPost(v1.getWeiboNegPost() + v2.getWeiboNegPost());
		entity.setWeiboNeuEngage(v1.getWeiboNeuEngage() + v2.getWeiboNeuEngage());
		entity.setWeiboNeuPost(v1.getWeiboNeuPost() + v2.getWeiboNeuPost());
		entity.setWeiboPosEngage(v1.getWeiboPosEngage() + v2.getWeiboPosEngage());
		entity.setWeiboPosPost(v1.getWeiboPosPost() + v2.getWeiboPosPost());
		entity.setWeiboTotalEngage(v1.getWeiboTotalEngage() + v2.getWeiboTotalEngage());
		entity.setWeiboTotalPost(v1.getWeiboTotalPost() + v2.getWeiboTotalPost());
		entity.setWeiboNegComments(v1.getWeiboNegComments() + v2.getWeiboNegComments());
		entity.setWeiboPosComments(v1.getWeiboPosComments() + v2.getWeiboPosComments());
		entity.setWeiboNeuComments(v1.getWeiboNeuComments() + v2.getWeiboNeuComments());
		entity.setWeiboTotalComments(v1.getWeiboTotalComments() + v2.getWeiboTotalComments());

		entity.setYoutubeNegEngage(v1.getYoutubeNegEngage() + v2.getYoutubeNegEngage());
		entity.setYoutubeNegPost(v1.getYoutubeNegPost() + v2.getYoutubeNegPost());
		entity.setYoutubeNeuEngage(v1.getYoutubeNeuEngage() + v2.getYoutubeNeuEngage());
		entity.setYoutubeNeuPost(v1.getYoutubeNeuPost() + v2.getYoutubeNeuPost());
		entity.setYoutubePosEngage(v1.getYoutubePosEngage() + v2.getYoutubePosEngage());
		entity.setYoutubePosPost(v1.getYoutubePosPost() + v2.getYoutubePosPost());
		entity.setYoutubeTotalEngage(v1.getYoutubeTotalEngage() + v2.getYoutubeTotalEngage());
		entity.setYoutubeTotalPost(v1.getYoutubeTotalPost() + v2.getYoutubeTotalPost());
		entity.setYoutubeNegComments(v1.getYoutubeNegComments() + v2.getYoutubeNegComments());
		entity.setYoutubePosComments(v1.getYoutubePosComments() + v2.getYoutubePosComments());
		entity.setYoutubeNeuComments(v1.getYoutubeNeuComments() + v2.getYoutubeNeuComments());
		entity.setYoutubeTotalComments(v1.getYoutubeTotalComments() + v2.getYoutubeTotalComments());

		entity.setInstagramNegEngage(v1.getInstagramNegEngage() + v2.getInstagramNegEngage());
		entity.setInstagramNegPost(v1.getInstagramNegPost() + v2.getInstagramNegPost());
		entity.setInstagramNeuEngage(v1.getInstagramNeuEngage() + v2.getInstagramNeuEngage());
		entity.setInstagramNeuPost(v1.getInstagramNeuPost() + v2.getInstagramNeuPost());
		entity.setInstagramPosEngage(v1.getInstagramPosEngage() + v2.getInstagramPosEngage());
		entity.setInstagramPosPost(v1.getInstagramPosPost() + v2.getInstagramPosPost());
		entity.setInstagramTotalEngage(v1.getInstagramTotalEngage() + v2.getInstagramTotalEngage());
		entity.setInstagramTotalPost(v1.getInstagramTotalPost() + v2.getInstagramTotalPost());
		entity.setInstagramNegComments(v1.getInstagramNegComments() + v2.getInstagramNegComments());
		entity.setInstagramPosComments(v1.getInstagramPosComments() + v2.getInstagramPosComments());
		entity.setInstagramNeuComments(v1.getInstagramNeuComments() + v2.getInstagramNeuComments());
		entity.setInstagramTotalComments(v1.getInstagramTotalComments() + v2.getInstagramTotalComments());

		entity.setWeixinNegEngage(v1.getWeixinNegEngage() + v2.getWeixinNegEngage());
		entity.setWeixinNegPost(v1.getWeixinNegPost() + v2.getWeixinNegPost());
		entity.setWeixinNeuEngage(v1.getWeixinNeuEngage() + v2.getWeixinNeuEngage());
		entity.setWeixinNeuPost(v1.getWeixinNeuPost() + v2.getWeixinNeuPost());
		entity.setWeixinPosEngage(v1.getWeixinPosEngage() + v2.getWeixinPosEngage());
		entity.setWeixinPosPost(v1.getWeixinPosPost() + v2.getWeixinPosPost());
		entity.setWeixinTotalEngage(v1.getWeixinTotalEngage() + v2.getWeixinTotalEngage());
		entity.setWeixinTotalPost(v1.getWeixinTotalPost() + v2.getWeixinTotalPost());
		entity.setWeixinNegComments(v1.getWeixinNegComments() + v2.getWeixinNegComments());
		entity.setWeixinPosComments(v1.getWeixinPosComments() + v2.getWeixinPosComments());
		entity.setWeixinNeuComments(v1.getWeixinNeuComments() + v2.getWeixinNeuComments());
		entity.setWeixinTotalComments(v1.getWeixinTotalComments() + v2.getWeixinTotalComments());

		entity.setPlurkNegEngage(v1.getPlurkNegEngage() + v2.getPlurkNegEngage());
		entity.setPlurkNegPost(v1.getPlurkNegPost() + v2.getPlurkNegPost());
		entity.setPlurkNeuEngage(v1.getPlurkNeuEngage() + v2.getPlurkNeuEngage());
		entity.setPlurkNeuPost(v1.getPlurkNeuPost() + v2.getPlurkNeuPost());
		entity.setPlurkPosEngage(v1.getPlurkPosEngage() + v2.getPlurkPosEngage());
		entity.setPlurkPosPost(v1.getPlurkPosPost() + v2.getPlurkPosPost());
		entity.setPlurkTotalEngage(v1.getPlurkTotalEngage() + v2.getPlurkTotalEngage());
		entity.setPlurkTotalPost(v1.getPlurkTotalPost() + v2.getPlurkTotalPost());
		entity.setPlurkNegComments(v1.getPlurkNegComments() + v2.getPlurkNegComments());
		entity.setPlurkPosComments(v1.getPlurkPosComments() + v2.getPlurkPosComments());
		entity.setPlurkNeuComments(v1.getPlurkNeuComments() + v2.getPlurkNeuComments());
		entity.setPlurkTotalComments(v1.getPlurkTotalComments() + v2.getPlurkTotalComments());

		entity.setLineqNegEngage(v1.getLineqNegEngage() + v2.getLineqNegEngage());
		entity.setLineqNegPost(v1.getLineqNegPost() + v2.getLineqNegPost());
		entity.setLineqNeuEngage(v1.getLineqNeuEngage() + v2.getLineqNeuEngage());
		entity.setLineqNeuPost(v1.getLineqNeuPost() + v2.getLineqNeuPost());
		entity.setLineqPosEngage(v1.getLineqPosEngage() + v2.getLineqPosEngage());
		entity.setLineqPosPost(v1.getLineqPosPost() + v2.getLineqPosPost());
		entity.setLineqTotalEngage(v1.getLineqTotalEngage() + v2.getLineqTotalEngage());
		entity.setLineqTotalPost(v1.getLineqTotalPost() + v2.getLineqTotalPost());
		entity.setLineqNegComments(v1.getLineqNegComments() + v2.getLineqNegComments());
		entity.setLineqPosComments(v1.getLineqPosComments() + v2.getLineqPosComments());
		entity.setLineqNeuComments(v1.getLineqNeuComments() + v2.getLineqNeuComments());
		entity.setLineqTotalComments(v1.getLineqTotalComments() + v2.getLineqTotalComments());

		entity.getDomain().addAll(v2.getDomain());
		entity.getDomainNeg().addAll(v2.getDomainNeg());
		entity.getDomainPos().addAll(v2.getDomainPos());
		entity.getDomainNeu().addAll(v2.getDomainNeu());

		entity.getPeople().addAll(v2.getPeople());
		entity.getPeopleNeg().addAll(v2.getPeopleNeg());
		entity.getPeoplePos().addAll(v2.getPeoplePos());
		entity.getPeopleNeu().addAll(v2.getPeopleNeu());

		return entity;
	}

}

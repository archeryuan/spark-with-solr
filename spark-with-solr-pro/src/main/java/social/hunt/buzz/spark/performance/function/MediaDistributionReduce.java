package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

import social.hunt.buzz.spark.data.MediaDistributionEntity;

public class MediaDistributionReduce implements Function2<MediaDistributionEntity, MediaDistributionEntity, MediaDistributionEntity>,
		Serializable {

	private static final long serialVersionUID = -4368927351350010093L;

	@Override
	public MediaDistributionEntity call(MediaDistributionEntity v1, MediaDistributionEntity v2) throws Exception {
		MediaDistributionEntity entity = new MediaDistributionEntity();
		entity.setFbNum(v1.getFbNum() + v2.getFbNum());
		entity.setIgNum(v1.getIgNum() + v2.getIgNum());
		entity.setTelegramNum(v1.getTelegramNum() + v2.getTelegramNum());
		entity.setTwitterNum(v1.getTelegramNum() + v2.getTelegramNum());
		entity.setWeiboNum(v1.getWeiboNum() + v2.getWeiboNum());
		entity.setYoutubeNum(v1.getYoutubeNum() + v2.getYoutubeNum());
		entity.setWebNum(v1.getWebNum() + v2.getWebNum());

		entity.setDate(v1.getDate());

		return entity;
	}

}

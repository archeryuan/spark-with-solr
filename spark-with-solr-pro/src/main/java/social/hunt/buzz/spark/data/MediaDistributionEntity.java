package social.hunt.buzz.spark.data;

import java.util.Date;

public class MediaDistributionEntity {
	private Date date = null;
	private long webNum = 0;
	private long fbNum = 0;
	private long igNum = 0;
	private long weiboNum = 0;
	private long twitterNum = 0;
	private long youtubeNum = 0;
	private long telegramNum = 0;

	public Date getDate() {
		return date;
	}

	public long getWebNum() {
		return webNum;
	}

	public long getFbNum() {
		return fbNum;
	}

	public long getIgNum() {
		return igNum;
	}

	public long getWeiboNum() {
		return weiboNum;
	}

	public long getTwitterNum() {
		return twitterNum;
	}

	public long getYoutubeNum() {
		return youtubeNum;
	}

	public long getTelegramNum() {
		return telegramNum;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public void setWebNum(long webNum) {
		this.webNum = webNum;
	}

	public void setFbNum(long fbNum) {
		this.fbNum = fbNum;
	}

	public void setIgNum(long igNum) {
		this.igNum = igNum;
	}

	public void setWeiboNum(long weiboNum) {
		this.weiboNum = weiboNum;
	}

	public void setTwitterNum(long twitterNum) {
		this.twitterNum = twitterNum;
	}

	public void setYoutubeNum(long youtubeNum) {
		this.youtubeNum = youtubeNum;
	}

	public void setTelegramNum(long telegramNum) {
		this.telegramNum = telegramNum;
	}

}

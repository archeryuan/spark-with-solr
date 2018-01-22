package social.hunt.buzz.spark.data;

public class TopChannelEntity {
	private String media = null;
	private String channel = null;
	private long egagement = 0;
	private long postNum = 0;
	private long themeNum = 0;
	
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public long getEgagement() {
		return egagement;
	}
	public void setEgagement(long egagement) {
		this.egagement = egagement;
	}
	public long getPostNum() {
		return postNum;
	}
	public void setPostNum(long postNum) {
		this.postNum = postNum;
	}
	public long getThemeNum() {
		return themeNum;
	}
	public void setThemeNum(long themeNum) {
		this.themeNum = themeNum;
	}
	public String getMedia() {
		return media;
	}
	public void setMedia(String media) {
		this.media = media;
	}
}

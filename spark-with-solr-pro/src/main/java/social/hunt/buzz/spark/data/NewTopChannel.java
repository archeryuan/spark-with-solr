package social.hunt.buzz.spark.data;

public class NewTopChannel {
	private String media = null;
	private String channel = null;
	private String channelUrl = null;
	private String uId = null;
	
	private long engageNeg = 0;
	private long engageNeu = 0;
	private long engagePos = 0;
	private long engageTotal = 0;
	private long postNumNeg = 0;
	private long postNumNeu = 0;
	private long postNumPos = 0;
	private long postNumTotal = 0;
	
	private long commentNeg = 0;
	private long commentNeu = 0;
	private long commentPos = 0;
	private long commentTotal = 0;
	
	private String negthemes = "NA";
	private String neuthemes = "NA";
	private String posthemes = "NA";
	
	public String getMedia() {
		return media;
	}
	public void setMedia(String media) {
		this.media = media;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public long getEngageNeg() {
		return engageNeg;
	}
	public void setEngageNeg(long engageNeg) {
		this.engageNeg = engageNeg;
	}
	public long getEngageNeu() {
		return engageNeu;
	}
	public void setEngageNeu(long engageNeu) {
		this.engageNeu = engageNeu;
	}
	public long getEngagePos() {
		return engagePos;
	}
	public void setEngagePos(long engagePos) {
		this.engagePos = engagePos;
	}
	public long getEngageTotal() {
		return engageTotal;
	}
	public void setEngageTotal(long engageTotal) {
		this.engageTotal = engageTotal;
	}
	public long getPostNumNeg() {
		return postNumNeg;
	}
	public void setPostNumNeg(long postNumNeg) {
		this.postNumNeg = postNumNeg;
	}
	public long getPostNumNeu() {
		return postNumNeu;
	}
	public void setPostNumNeu(long postNumNeu) {
		this.postNumNeu = postNumNeu;
	}
	public long getPostNumPos() {
		return postNumPos;
	}
	public void setPostNumPos(long postNumPos) {
		this.postNumPos = postNumPos;
	}
	public long getPostNumTotal() {
		return postNumTotal;
	}
	public void setPostNumTotal(long postNumTotal) {
		this.postNumTotal = postNumTotal;
	}
	public String getNegthemes() {
		return negthemes;
	}
	public void setNegthemes(String negthemes) {
		this.negthemes = negthemes;
	}
	public String getNeuthemes() {
		return neuthemes;
	}
	public void setNeuthemes(String neuthemes) {
		this.neuthemes = neuthemes;
	}
	public String getPosthemes() {
		return posthemes;
	}
	public void setPosthemes(String posthemes) {
		this.posthemes = posthemes;
	}
	public String getChannelUrl() {
		return channelUrl;
	}
	public void setChannelUrl(String channelUrl) {
		this.channelUrl = channelUrl;
	}
	public String getuId() {
		return uId;
	}
	public void setuId(String uId) {
		this.uId = uId;
	}
	public long getCommentNeg() {
		return commentNeg;
	}
	public void setCommentNeg(long commentNeg) {
		this.commentNeg = commentNeg;
	}
	public long getCommentNeu() {
		return commentNeu;
	}
	public void setCommentNeu(long commentNeu) {
		this.commentNeu = commentNeu;
	}
	public long getCommentPos() {
		return commentPos;
	}
	public void setCommentPos(long commentPos) {
		this.commentPos = commentPos;
	}
	public long getCommentTotal() {
		return commentTotal;
	}
	public void setCommentTotal(long commentTotal) {
		this.commentTotal = commentTotal;
	}	
	
}

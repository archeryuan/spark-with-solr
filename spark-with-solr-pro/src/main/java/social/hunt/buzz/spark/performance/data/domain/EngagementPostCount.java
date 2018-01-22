package social.hunt.buzz.spark.performance.data.domain;

import java.io.Serializable;

public class EngagementPostCount implements Serializable  {

	private static final long serialVersionUID = 6426673461170845159L;
	
	private Long engagement;
	private Long postCount;

	public EngagementPostCount() {
	}

	public EngagementPostCount(Long engagement, Long postCount) {
		this.engagement = engagement;
		this.postCount = postCount;
	}

	public Long getEngagement() {
		return engagement;
	}

	public void setEngagement(Long engagement) {
		this.engagement = engagement;
	}

	public Long getPostCount() {
		return postCount;
	}

	public void setPostCount(Long postCount) {
		this.postCount = postCount;
	}

}

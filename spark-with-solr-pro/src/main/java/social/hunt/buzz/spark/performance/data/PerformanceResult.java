/**
 * 
 */
package social.hunt.buzz.spark.performance.data;

import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author lewis
 *
 */
public class PerformanceResult {
	@JsonProperty("DOCUMENT_COUNT")
	private long documentCount;
	// @JsonProperty("POSITIVE_SENTIMENT")
	// private long positiveSentiment;
	// @JsonProperty("NEGATIVE_SENTIMENT")
	// private long negativeSentiment;
	// @JsonProperty("NRUTRAL_SENTIMENT")
	// private long neutralSentiment;
	@JsonProperty("ENGAGEMENT_SCORE")
	private long engagementScore;
	@JsonProperty("UNIQUE_USER_COUNT")
	private long uniqueUserCount;
	@JsonProperty("COVERAGE")
	private Map<String, Long> coverage;
	@JsonProperty("SENTIMENT_SCORE")
	private double sentimentScore;

	/**
	 * 
	 */
	public PerformanceResult() {
		super();
	}

	/**
	 * @return the documentCount
	 */
	public long getDocumentCount() {
		return documentCount;
	}

	/**
	 * @param documentCount
	 *            the documentCount to set
	 */
	public void setDocumentCount(long documentCount) {
		this.documentCount = documentCount;
	}

	/**
	 * @return the engagementScore
	 */
	public long getEngagementScore() {
		return engagementScore;
	}

	/**
	 * @param engagementScore
	 *            the engagementScore to set
	 */
	public void setEngagementScore(long engagementScore) {
		this.engagementScore = engagementScore;
	}

	/**
	 * @return the uniqueUserCount
	 */
	public long getUniqueUserCount() {
		return uniqueUserCount;
	}

	/**
	 * @param uniqueUserCount
	 *            the uniqueUserCount to set
	 */
	public void setUniqueUserCount(long uniqueUserCount) {
		this.uniqueUserCount = uniqueUserCount;
	}

	/**
	 * @return the coverage
	 */
	public Map<String, Long> getCoverage() {
		return coverage;
	}

	/**
	 * @param coverage
	 *            the coverage to set
	 */
	public void setCoverage(Map<String, Long> coverage) {
		this.coverage = coverage;
	}

	/**
	 * @return the sentimentScore
	 */
	public double getSentimentScore() {
		return sentimentScore;
	}

	/**
	 * @param sentimentScore
	 *            the sentimentScore to set
	 */
	public void setSentimentScore(double sentimentScore) {
		this.sentimentScore = sentimentScore;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}

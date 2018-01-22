/**
 * 
 */
package social.hunt.buzz.spark.sentiment.data;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import social.hunt.buzz.spark.data.JobInput;
import social.hunt.data.domain.DashboardProfile;

/**
 * @author lewis
 *
 */
public class SentimentAnalysisJobInput extends JobInput {

	/**
	 * Buzz keyword profile
	 */
	private DashboardProfile profile;

	/**
	 * Number of day to analyze
	 */
	private int numDayToAnalyze;

	/**
	 * 
	 */
	public SentimentAnalysisJobInput() {
	}

	/**
	 * @param taskId
	 */
	public SentimentAnalysisJobInput(String taskId) {
		super(taskId);
	}

	/**
	 * @return the profile
	 */
	public DashboardProfile getProfile() {
		return profile;
	}

	/**
	 * @param profile
	 *            the profile to set
	 */
	public void setProfile(DashboardProfile profile) {
		this.profile = profile;
	}

	/**
	 * @return the numDayToAnalyze
	 */
	public int getNumDayToAnalyze() {
		return numDayToAnalyze;
	}

	/**
	 * @param numDayToAnalyze
	 *            the numDayToAnalyze to set
	 */
	public void setNumDayToAnalyze(int numDayToAnalyze) {
		this.numDayToAnalyze = numDayToAnalyze;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}

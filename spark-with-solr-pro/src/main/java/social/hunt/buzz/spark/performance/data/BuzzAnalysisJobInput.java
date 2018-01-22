/**
 * 
 */
package social.hunt.buzz.spark.performance.data;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import social.hunt.buzz.spark.data.JobInput;
import social.hunt.data.domain.BuzzAnalysisData;
import social.hunt.data.domain.DashboardProfile;

/**
 * Task input container for Spark Buzz Analysis
 * 
 * @author lewis
 *
 */
public class BuzzAnalysisJobInput extends JobInput {

	// @JsonProperty("PROFILE")
	private DashboardProfile profile;
	// @JsonProperty("DATA_LIST")
	private List<BuzzAnalysisData> dataList;
	private Date startDate;
	private Date endDate;

	/**
	 * 
	 */
	public BuzzAnalysisJobInput() {
		super();
	}

	/**
	 * @param profile
	 * @param dataList
	 */
	public BuzzAnalysisJobInput(String taskId, DashboardProfile profile, List<BuzzAnalysisData> dataList) {
		super(taskId);
		this.profile = profile;
		this.dataList = dataList;
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
	 * @return the dataList
	 */
	public List<BuzzAnalysisData> getDataList() {
		return dataList;
	}

	/**
	 * @param dataList
	 *            the dataList to set
	 */
	public void setDataList(List<BuzzAnalysisData> dataList) {
		this.dataList = dataList;
	}
	

	public Date getStartDate() {
		return startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}

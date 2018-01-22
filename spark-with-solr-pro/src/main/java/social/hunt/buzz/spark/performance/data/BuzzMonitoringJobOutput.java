/**
 * 
 */
package social.hunt.buzz.spark.performance.data;

import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import social.hunt.buzz.spark.data.JobOutput;
import social.hunt.buzz.spark.performance.data.domain.MonitorKeyValueData;

/**
 * Task output container for Spark Buzz Analysis
 * 
 * @author lewis
 *
 */
public class BuzzMonitoringJobOutput extends JobOutput {

	// @JsonProperty("POST_LIST")
	private List<MonitorKeyValueData> dailyPostList;

	// @JsonProperty("TOP_POST")
	private List<MonitorKeyValueData> topPostList;

	// @JsonProperty("POST_COUNT")
	private Long postCount;

	/**
	 * 
	 */
	public BuzzMonitoringJobOutput() {
		super();
	}

	public BuzzMonitoringJobOutput(List<MonitorKeyValueData> dataList, int status) {
		super(status);
		this.dailyPostList = dataList;
	}

	public List<MonitorKeyValueData> getDailyPostList() {
		return dailyPostList;
	}

	public void setDailyPostList(List<MonitorKeyValueData> dailyPostList) {
		this.dailyPostList = dailyPostList;
	}

	public List<MonitorKeyValueData> getTopPostList() {
		return topPostList;
	}

	public void setTopPostList(List<MonitorKeyValueData> topPostList) {
		this.topPostList = topPostList;
	}

	public Long getPostCount() {
		return postCount;
	}

	public void setPostCount(Long postCount) {
		this.postCount = postCount;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}

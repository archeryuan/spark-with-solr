/**
 * 
 */
package social.hunt.buzz.spark.performance.data;

import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import social.hunt.buzz.spark.data.JobOutput;
import social.hunt.buzz.spark.data.MediaDistributionEntity;
import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.data.domain.BuzzAnalysisData;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Task output container for Spark Buzz Analysis
 * 
 * @author lewis
 *
 */
public class BuzzAnalysisJobOutput extends JobOutput {

	@JsonProperty("DATA_LIST")
	private List<BuzzAnalysisData> dataList;

	@JsonProperty("TOPCHANNEL_LIST")
	private List<TopChannelEntity> topChannelList;

	@JsonProperty("MEDIA_DISTRIBUTION_LIST")
	private List<MediaDistributionEntity> mediaDistriList;

	/**
	 * 
	 */
	public BuzzAnalysisJobOutput() {
		super();
	}

	/**
	 * @param dataList
	 */
	public BuzzAnalysisJobOutput(List<BuzzAnalysisData> dataList, List<TopChannelEntity> topChannelList, int status) {
		super(status);
		this.dataList = dataList;
		this.topChannelList = topChannelList;
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

	public List<TopChannelEntity> getTopChannelList() {
		return topChannelList;
	}

	public void setTopChannelList(List<TopChannelEntity> topChannelList) {
		this.topChannelList = topChannelList;
	}

	public List<MediaDistributionEntity> getMediaDistriList() {
		return mediaDistriList;
	}

	public void setMediaDistriList(List<MediaDistributionEntity> mediaDistriList) {
		this.mediaDistriList = mediaDistriList;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}

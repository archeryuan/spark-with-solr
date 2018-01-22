/**
 * 
 */
package social.hunt.buzz.spark.sentiment.data;

import java.util.List;

import social.hunt.buzz.spark.data.JobOutput;
import social.hunt.data.domain.SentimentAnalysisData;

/**
 * @author lewis
 *
 */
public class SentimentAnalysisJobOutput extends JobOutput {

	private List<SentimentAnalysisData> dataList;

	/**
	 * 
	 */
	public SentimentAnalysisJobOutput() {
	}

	/**
	 * @param status
	 */
	public SentimentAnalysisJobOutput(int status) {
		super(status);
	}

	/**
	 * @return the dataList
	 */
	public List<SentimentAnalysisData> getDataList() {
		return dataList;
	}

	/**
	 * @param dataList
	 *            the dataList to set
	 */
	public void setDataList(List<SentimentAnalysisData> dataList) {
		this.dataList = dataList;
	}

}

package social.hunt.buzz.spark.data;

import java.io.Serializable;

public class SOVEntity implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -6666099821237708475L;
	private Long engagement;
	private Long volume;
	private String catName;
	private String dateStr;

	public Long getEngagement() {
		return engagement;
	}

	public Long getVolume() {
		return volume;
	}

	public void setEngagement(Long engagement) {
		this.engagement = engagement;
	}

	public void setVolume(Long volume) {
		this.volume = volume;
	}

	public String getCatName() {
		return catName;
	}

	public void setCatName(String catName) {
		this.catName = catName;
	}

	public String getDateStr() {
		return dateStr;
	}

	public void setDateStr(String dateStr) {
		this.dateStr = dateStr;
	}

}

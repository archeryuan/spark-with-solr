/**
 * 
 */
package social.hunt.buzz.spark.data;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author lewis
 *
 */
public class JobInput {

	@JsonProperty("TASK_ID")
	private String taskId;

	/**
	 * 
	 */
	public JobInput() {
	}

	/**
	 * @param taskId
	 */
	public JobInput(String taskId) {
		super();
		this.taskId = taskId;
	}

	/**
	 * @return the taskId
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * @param taskId
	 *            the taskId to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}

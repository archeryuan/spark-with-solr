/**
 * 
 */
package social.hunt.buzz.spark.data;

import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author lewis
 *
 */
public class JobOutput {
	public static final int SUCCESS = 1;
	public static final int FAIL = 2;

	@JsonProperty("STATUS")
	private int status;

	@JsonProperty("TASK_ID")
	private String taskId;

	@JsonProperty("EXCEPTIONS")
	private List<String> exceptions;

	/**
	 * 
	 */
	public JobOutput() {
		super();
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

	/**
	 * @param status
	 */
	public JobOutput(int status) {
		super();
		this.status = status;
	}

	/**
	 * @return the status
	 */
	public int getStatus() {
		return status;
	}

	/**
	 * @param status
	 *            the status to set
	 */
	public void setStatus(int status) {
		this.status = status;
	}

	/**
	 * @return the exceptions
	 */
	public List<String> getExceptions() {
		return exceptions;
	}

	/**
	 * @param exceptions
	 *            the exceptions to set
	 */
	public void setExceptions(List<String> exceptions) {
		this.exceptions = exceptions;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}

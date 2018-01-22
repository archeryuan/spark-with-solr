package social.hunt.buzz.spark.performance.data.domain;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

public class MonitorKeyValueData {

	private Object key;
	private EngagementPostCount value;

	public MonitorKeyValueData() {

	}

	public MonitorKeyValueData(Object key, EngagementPostCount value) {
		this.key = key;
		this.value = value;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public EngagementPostCount getValue() {
		return value;
	}

	public void setValue(EngagementPostCount value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}

package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

import social.hunt.buzz.spark.data.SOVEntity;

public class SovReduce implements Function2<SOVEntity, SOVEntity, SOVEntity>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1404455577468319040L;

	@Override
	public SOVEntity call(SOVEntity v1, SOVEntity v2) throws Exception {
		SOVEntity entity = v1;
		entity.setEngagement(v1.getEngagement() + v2.getEngagement());
		entity.setVolume(v1.getVolume() + v2.getVolume());
		return entity;
	}
}

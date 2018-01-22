/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.spark.api.java.function.Function2;

import social.hunt.buzz.spark.data.TopChannelEntity;

/**
 * @author lewis
 *
 */
public class TopChannelReduce implements Function2<TopChannelEntity, TopChannelEntity, TopChannelEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8906661414607659632L;

	/**
	 * 
	 */
	public TopChannelReduce() {
	}

	@Override
	public TopChannelEntity call(TopChannelEntity v1, TopChannelEntity v2) throws Exception {
		TopChannelEntity entity = new TopChannelEntity();
		
		entity.setChannel(v1.getChannel());
		entity.setEgagement(v1.getEgagement() + v2.getEgagement());
		entity.setPostNum(v1.getPostNum() + v2.getPostNum());
		entity.setThemeNum(v1.getThemeNum() + v2.getThemeNum());
		return entity;
	}

}

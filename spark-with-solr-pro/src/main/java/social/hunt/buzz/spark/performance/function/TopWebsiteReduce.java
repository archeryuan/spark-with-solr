/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.spark.api.java.function.Function2;

import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.buzz.spark.data.TopWebsiteEntity;

/**
 * @author archer
 *
 */
public class TopWebsiteReduce implements Function2<TopWebsiteEntity, TopWebsiteEntity, TopWebsiteEntity>, Serializable {


	private static final long serialVersionUID = -8906661414607659632L;

	public TopWebsiteReduce() {
	}

	@Override
	public TopWebsiteEntity call(TopWebsiteEntity v1, TopWebsiteEntity v2) throws Exception {
		TopWebsiteEntity entity = v1;
		
		entity.setEngageNeg(v1.getEngageNeg() + v2.getEngageNeg());
		entity.setEngageNeu(v1.getEngageNeu() + v2.getEngageNeu());
		entity.setEngagePos(v1.getEngagePos() + v2.getEngagePos());
		entity.setEngageTotal(v1.getEngageTotal() + v2.getEngageTotal());
		
		entity.setCommentNeg(v1.getCommentNeg() + v2.getCommentNeg());
		entity.setCommentNeu(v1.getCommentNeu() + v2.getCommentNeu());
		entity.setCommentPos(v1.getCommentPos() + v2.getCommentPos());
		entity.setCommentTotal(v1.getCommentTotal() + v2.getCommentTotal());
		
		entity.setPostNeg(v1.getPostNeg() + v2.getPostNeg());
		entity.setPostNeu(v1.getPostNeu() + v2.getPostNeu());
		entity.setPostPos(v1.getPostPos() + v2.getPostPos());
		entity.setPostTotal(v1.getPostTotal() + v2.getPostTotal());
		return entity;
	}

}

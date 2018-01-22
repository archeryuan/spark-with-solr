/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.spark.api.java.function.Function2;

import social.hunt.buzz.spark.data.NewTopChannel;
import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.common.definition.Sns;

/**
 * @author archer
 *
 */
public class NewTopChannelReduce implements Function2<NewTopChannel, NewTopChannel, NewTopChannel>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8906661414607659632L;

	/**
	 * 
	 */
	public NewTopChannelReduce() {
	}

	@Override
	public NewTopChannel call(NewTopChannel v1, NewTopChannel v2) throws Exception {
		NewTopChannel entity = v1;

		entity.setEngageNeg(v1.getEngageNeg() + v2.getEngageNeg());
		entity.setEngageNeu(v1.getEngageNeu() + v2.getEngageNeu());
		entity.setEngagePos(v1.getEngagePos() + v2.getEngagePos());
		entity.setEngageTotal(v1.getEngageTotal() + v2.getEngageTotal());
		entity.setCommentNeg(v1.getCommentNeg() + v2.getCommentNeg());
		entity.setCommentNeu(v1.getCommentNeu() + v2.getCommentNeu());
		entity.setCommentPos(v1.getCommentPos() + v2.getCommentPos());
		entity.setCommentTotal(v1.getCommentTotal() + v2.getCommentTotal());
		
		entity.setPostNumNeg(v1.getPostNumNeg() + v2.getPostNumNeg());
		entity.setPostNumNeu(v1.getPostNumNeu() + v2.getPostNumNeu());
		entity.setPostNumPos(v1.getPostNumPos() + v2.getPostNumPos());
		
		entity.setPostNumTotal(v1.getPostNumTotal() + v2.getPostNumTotal());
		
		if(entity.getNegthemes().equals("NA")){
			if(!v2.getNegthemes().equals("NA")){
				entity.setNegthemes(v2.getNegthemes());
			}
		}
		
		if(entity.getNeuthemes().equals("NA")){
			if(!v2.getNeuthemes().equals("NA")){
				entity.setNeuthemes(v2.getNeuthemes());
			}
		}
		
		if(entity.getPosthemes().equals("NA")){
			if(!v2.getPosthemes().equals("NA")){
				entity.setNegthemes(v2.getPosthemes());
			}
		}
		
		return entity;
	}

}

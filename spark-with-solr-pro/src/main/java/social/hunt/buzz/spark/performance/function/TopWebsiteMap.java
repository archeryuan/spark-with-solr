package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.buzz.spark.data.TopWebsiteEntity;

import com.sa.common.definition.SolrFieldDefinition;

public class TopWebsiteMap implements PairFunction<SolrDocument, String, TopWebsiteEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3778479205840134893L;

	@Override
	public Tuple2<String, TopWebsiteEntity> call(SolrDocument doc) {
		String domain =  null;
		TopWebsiteEntity entity = new TopWebsiteEntity();

		if (doc != null) {
			long readCnt = 0;
			long commentCnt = 0;
			long shareCnt = 0;
			float sScore = 0;
			
			if (doc.containsKey(SolrFieldDefinition.DOMAIN.getName())) {
				domain = (String)doc.getFieldValue(SolrFieldDefinition.DOMAIN.getName());				
			}
			
			if (doc.containsKey(SolrFieldDefinition.READ_COUNT.getName())){
				readCnt = (long)doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
			}	
			
			if (doc.containsKey(SolrFieldDefinition.COMMENT_COUNT.getName())){
				commentCnt = (long)doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
			}	
			
			if (doc.containsKey(SolrFieldDefinition.SHARE_COUNT.getName())){
				shareCnt = (long)doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
			}	
			
			if (doc.containsKey(SolrFieldDefinition.SENTIMENT_SCORE.getName())) {
				sScore = (float) doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
			}

			
			if(sScore < -0.45){
				entity.setEngageNeg(readCnt + commentCnt + shareCnt + 1);
				entity.setPostNeg(1);
				entity.setCommentNeg(commentCnt);
			}else if(sScore > 0.5){
				entity.setEngagePos(readCnt + commentCnt + shareCnt + 1);
				entity.setPostPos(1);
				entity.setCommentPos(commentCnt);
			}else{
				entity.setEngageNeu(readCnt + commentCnt + shareCnt + 1);
				entity.setPostNeu(1);
				entity.setCommentNeu(commentCnt);
			}
			
			entity.setCommentTotal(commentCnt);
			entity.setEngageTotal(readCnt + commentCnt + shareCnt + 1);
			entity.setPostTotal(1);
			entity.setDomain(domain);
		}
		return new Tuple2<String, TopWebsiteEntity>(domain, entity);
	}

}

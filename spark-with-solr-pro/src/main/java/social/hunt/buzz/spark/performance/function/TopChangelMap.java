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

import com.sa.common.definition.SolrFieldDefinition;

public class TopChangelMap implements PairFunction<SolrDocument, String, TopChannelEntity>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3778479205840134893L;

	@Override
	public Tuple2<String, TopChannelEntity> call(SolrDocument doc) {
		String uName =  null;
		TopChannelEntity entity = new TopChannelEntity();

		if (doc != null) {
			long readCnt = 0;
			long commentCnt = 0;
			long shareCnt = 0;
			long negTheme = 0;
			long posTheme = 0;	
			long neuTheme = 0;
			
			if (doc.containsKey(SolrFieldDefinition.USER_NAME.getName()) && doc.containsKey(SolrFieldDefinition.SNS_TYPE.getName())) {
				uName = (int)doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName())+"_" + (String) doc.getFieldValue(SolrFieldDefinition.USER_NAME.getName());				
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
			
			if (doc.containsKey(SolrFieldDefinition.POS_THEMES.getName())) {
				List<String> posThemes = (List<String>) doc.getFieldValue(SolrFieldDefinition.POS_THEMES.getName());
				posTheme = posThemes.size();
			}
			if (doc.containsKey(SolrFieldDefinition.NEG_THEMES.getName())) {
				List<String> negThemes = (List<String>) doc.getFieldValue(SolrFieldDefinition.NEG_THEMES.getName());
				negTheme = negThemes.size();
			}
			if (doc.containsKey(SolrFieldDefinition.NEUTRAL_THEMES.getName())) {
				List<String>neuThemes = (List<String>) doc.getFieldValue(SolrFieldDefinition.NEUTRAL_THEMES.getName());
				neuTheme = neuThemes.size();
			}
			
			entity.setChannel(uName);
			entity.setEgagement(readCnt + commentCnt + shareCnt);
			entity.setPostNum(1);
			entity.setThemeNum(neuTheme + negTheme + posTheme);
		}
		return new Tuple2<String, TopChannelEntity>(uName, entity);
	}

}

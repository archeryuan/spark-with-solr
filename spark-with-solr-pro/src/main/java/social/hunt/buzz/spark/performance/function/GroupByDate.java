package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.sa.common.definition.SolrFieldDefinition;

public class GroupByDate implements PairFunction<SolrDocument, String, SolrDocument>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3778479205840134893L;

	@Override
	public Tuple2<String, SolrDocument> call(SolrDocument doc) {
		String reportDate =  null;

		if (doc != null) {
	
			if (doc.containsKey(SolrFieldDefinition.PUBLISH_DATE.getName())) {
				Date pDate = (Date) doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
				if(pDate != null){
					Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
					c.setTime(pDate);
					c.set(Calendar.HOUR_OF_DAY, 0);
					c.set(Calendar.MINUTE, 0);
					c.set(Calendar.SECOND, 0);
					c.set(Calendar.MILLISECOND, 0);
					pDate = c.getTime();
					SimpleDateFormat df = new SimpleDateFormat("MMddyyyy");
					reportDate = df.format(pDate);
				}else
					reportDate = "nullpDate";
			}else{
				reportDate = "noPDate";
			}
		}else{
			reportDate = "nullDoc";
		}
		return new Tuple2<String, SolrDocument>(reportDate, doc);
	}

}

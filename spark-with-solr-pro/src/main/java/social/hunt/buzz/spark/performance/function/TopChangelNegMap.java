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
import social.hunt.buzz.spark.data.NewTopChannel;
import social.hunt.buzz.spark.data.TopChannelEntity;

import com.sa.common.definition.SolrFieldDefinition;

public class TopChangelNegMap implements PairFunction<Tuple2<String, NewTopChannel>, Long, NewTopChannel>, Serializable {

	private static final long serialVersionUID = -3778479205840134893L;

	@Override
	public Tuple2<Long, NewTopChannel> call(Tuple2<String, NewTopChannel> tuple) {
		return new Tuple2<Long, NewTopChannel>(tuple._2.getEngageNeg(), tuple._2);
	}
}

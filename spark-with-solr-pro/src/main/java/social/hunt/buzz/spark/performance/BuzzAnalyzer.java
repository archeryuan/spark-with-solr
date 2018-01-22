/**
 * 
 */
package social.hunt.buzz.spark.performance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import social.hunt.buzz.spark.common.SparkProfileAnalyzer;
import social.hunt.buzz.spark.common.function.SumLong;
import social.hunt.buzz.spark.performance.data.BuzzAnalysisJobInput;
import social.hunt.buzz.spark.performance.data.BuzzAnalysisJobOutput;
import social.hunt.buzz.spark.performance.data.PerformanceResult;
import social.hunt.buzz.spark.performance.definition.ResultType;
import social.hunt.buzz.spark.performance.function.CountingLong;
import social.hunt.buzz.spark.performance.function.CoveragePairFunction;
import social.hunt.buzz.spark.performance.function.FunctionUtils;
import social.hunt.buzz.spark.performance.function.NewWorldCountLong;
import social.hunt.buzz.spark.performance.function.SentimentMap;
import social.hunt.buzz.spark.performance.function.SentimentReduce;
import social.hunt.buzz.spark.performance.function.UserCount;
import social.hunt.buzz.spark.performance.function.harbourrace.HRCoveragtePairFunction;
import social.hunt.data.domain.BuzzAnalysisData;
import social.hunt.data.domain.DashboardProfile;
import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.SolrQueryUtil;

import com.lucidworks.spark.SolrRDD;
import com.sa.common.config.CommonConfig;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.DateUtils;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.definition.RedisDefinition.SparkDef;
import com.sa.redis.util.RedisUtil;

/**
 * @author lewis
 *
 */
public class BuzzAnalyzer extends SparkProfileAnalyzer<BuzzAnalysisJobInput> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8800074206210013504L;
	public static final int MAX_RECORD = 60;
	// private static final int DECIMAL_PLACE = 8;

	private static final Logger log = LoggerFactory.getLogger(BuzzAnalyzer.class);

	private final String appName;
	private final JavaSparkContext context;
	private final DashboardProfile profile;
	private final BuzzAnalysisJobInput jobInput;
	private final String taskId;

	private List<String> exceptions;
	private SparkConf sparkConf;

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			log.warn("Input is empty!");
			return;
		}

		long startMs = System.currentTimeMillis();

		/**
		 * args[0] - taskId<BR>
		 * JobInput object in Redis
		 */

		BuzzAnalyzer instance = null;

		try {
			String taskId = args[0];

			instance = new BuzzAnalyzer(taskId);
			List<BuzzAnalysisData> result = instance.analyze2();
			List<String> exceptions = instance.getExceptions();

			BuzzAnalysisJobOutput output = new BuzzAnalysisJobOutput();
			output.setTaskId(taskId);
			output.setDataList(result);
			if (exceptions.isEmpty()) {
				output.setStatus(BuzzAnalysisJobOutput.SUCCESS);
			} else {
				output.setStatus(BuzzAnalysisJobOutput.FAIL);
				output.setExceptions(exceptions);
			}

			// RedisUtil.getInstance().publish(taskId, getGson().toJson(output));
			instance.submitResult(output);

			// instance.customCalculation();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			if (instance != null)
				instance.shutdown();

			// log time spent
			if (log.isInfoEnabled())
				log.info("Time spent in ms: {}", (System.currentTimeMillis() - startMs));
		}
	}

	public void customCalculation() throws SolrServerException {
		if (profile.getId() >= 57 && profile.getId() <= 63) {
			// if (profile.getId() == 57) {
			log.info("Doing custom calculation for Harbour Race.");

			Date startDate = null;
			Date endDate = null;

			if (profile.getId() == 57 || profile.getId() == 61 || profile.getId() == 62) {
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				cal.set(Calendar.MONTH, Calendar.MAY);
				cal.set(Calendar.DAY_OF_MONTH, 24);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				startDate = cal.getTime();

				// cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				// cal.set(Calendar.MONTH, Calendar.SEPTEMBER);
				// cal.set(Calendar.DAY_OF_MONTH, 24);
				// cal.set(Calendar.HOUR, 0);
				// cal.set(Calendar.MINUTE, 0);
				// cal.set(Calendar.SECOND, 0);
				// cal.set(Calendar.MILLISECOND, 0);

				cal.set(Calendar.MONTH, Calendar.NOVEMBER);
				cal.set(Calendar.DAY_OF_MONTH, 5);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				endDate = DateUtils.getEnd(cal.getTime());
			} else if (profile.getId() == 60) {
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				cal.set(Calendar.YEAR, 2014);
				cal.set(Calendar.MONTH, Calendar.OCTOBER);
				cal.set(Calendar.DAY_OF_MONTH, 24);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				startDate = cal.getTime();

				cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				cal.set(Calendar.YEAR, 2015);
				cal.set(Calendar.MONTH, Calendar.JANUARY);
				cal.set(Calendar.DAY_OF_MONTH, 31);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				endDate = cal.getTime();
			} else if (profile.getId() == 59) {
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				cal.set(Calendar.YEAR, 2014);
				cal.set(Calendar.MONTH, Calendar.SEPTEMBER);
				cal.set(Calendar.DAY_OF_MONTH, 14);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				startDate = cal.getTime();

				cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				cal.set(Calendar.YEAR, 2014);
				cal.set(Calendar.MONTH, Calendar.DECEMBER);
				cal.set(Calendar.DAY_OF_MONTH, 31);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				endDate = cal.getTime();
			} else if (profile.getId() == 63) {
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				cal.set(Calendar.MONTH, Calendar.JANUARY);
				cal.set(Calendar.DAY_OF_MONTH, 1);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				startDate = cal.getTime();

				cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
				cal.set(Calendar.MONTH, Calendar.APRIL);
				cal.set(Calendar.DAY_OF_MONTH, 15);
				cal.set(Calendar.HOUR, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				endDate = cal.getTime();
			}

			JavaRDD<SolrDocument> allDocs = loadAllDocRddForCustomCalculation(startDate, endDate).persist(StorageLevel.MEMORY_AND_DISK());

			// Cal total coverage, top 5 sites and contributor
			JavaPairRDD<ResultType, Long> javaPairRdd = allDocs.flatMapToPair(new NewWorldCountLong());
			javaPairRdd = javaPairRdd.reduceByKey(new SumLong());
			Map<ResultType, Long> resultLong = javaPairRdd.collectAsMap();

			/**
			 * Coverage
			 */
			JavaPairRDD<String, Long> coveragePair = allDocs.mapToPair(new HRCoveragtePairFunction()).reduceByKey(new SumLong());
			Map<String, Long> coverageResult = coveragePair.collectAsMap();

			// Cal no. of participant
			JavaPairRDD<String, Long> participantPair = allDocs.mapToPair(new ParticipantPairFunction()).reduceByKey(new SumLong());
			participantPair.cache();
			long pCount = participantPair.count();

			List<Tuple2<String, Long>> topN = participantPair.top(20, new CompareByValueLong());

			participantPair.unpersist();

			JavaPairRDD<String, Long> keywordPair = allDocs.flatMapToPair(new KeywordPairFunction()).reduceByKey(new SumLong());
			List<Tuple2<String, Long>> topNKeywords = keywordPair.top(100, new CompareByValueLong());

			// Sentiment %
			/**
			 * Calculate sentiment score
			 */
			JavaRDD<Double> sentimentRdd = allDocs.map(new SentimentMap());
			double sentimentScore = 0;
			if (!sentimentRdd.isEmpty())
				sentimentScore = sentimentRdd.reduce(new SentimentReduce());

			allDocs.unpersist();

			log.info("Document count: {}", resultLong.get(ResultType.DOCUMENT_COUNT));
			log.info("Positive count: {}", resultLong.get(ResultType.POSITIVE_COUNT));
			log.info("Negative count: {}", resultLong.get(ResultType.NEGATIVE_COUNT));
			log.info("No. of mention: {}", resultLong.get(ResultType.DOCUMENT_COUNT) + resultLong.get(ResultType.ENGAGEMENT_SCORE));

			log.info("*****No. of domain: {}", coverageResult.keySet().size());
			// log.info("*****Domains: {}", coverageResult.keySet());
			if (coverageResult != null && !coverageResult.isEmpty())
				for (String domain : coverageResult.keySet()) {
					log.info("{}\t{}", domain, coverageResult.get(domain));
				}

			// log.info("******Top 50 Domain");
			// for (Tuple2<String, Long> t : topN) {
			// log.info("{}\t{}", t._1(), t._2());
			// }

			log.info("******Sentiment Score: {}", sentimentScore);

			log.info("******No. of participant: {}", pCount);

			log.info("******Top 5 Contributor");
			for (Tuple2<String, Long> t : topN) {
				log.info("Domain+Contributor: {}, Count: {}", t._1(), t._2());
			}

			log.info("******Top N Keyowrds");
			for (Tuple2<String, Long> t : topNKeywords) {
				log.info("{}\t{}", t._1(), t._2());
			}
		}
	}

	public static class KeywordPairFunction extends FunctionUtils implements PairFlatMapFunction<SolrDocument, String, Long>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = -4132457559696232931L;

		@Override
		public Iterable<Tuple2<String, Long>> call(SolrDocument doc) throws Exception {
			ArrayList<Tuple2<String, Long>> result = new ArrayList<Tuple2<String, Long>>();
			String domain = extractDomainFromDoc(doc);

			if (StringUtils.endsWith(domain, ".tw") || StringUtils.endsWith(domain, ".cn") || "zhidao.baidu.com".equals(domain)) {
				return result;
			}

			Set<String> keywordSet = new HashSet<String>();

			if (doc.containsKey(SolrFieldDefinition.KEYWORDS.getName())) {
				Collection<Object> keywords = doc.getFieldValues(SolrFieldDefinition.KEYWORDS.getName());
				if (keywords != null)
					for (Object obj : keywords) {
						if (obj instanceof String) {
							keywordSet.add((String) obj);
						}
					}
			}

			if (doc.containsKey(SolrFieldDefinition.POS_KEYWORDS.getName())) {
				Collection<Object> keywords = doc.getFieldValues(SolrFieldDefinition.POS_KEYWORDS.getName());
				if (keywords != null)
					for (Object obj : keywords) {
						if (obj instanceof String) {
							keywordSet.add((String) obj);
						}
					}
			}

			if (doc.containsKey(SolrFieldDefinition.NEG_KEYWORDS.getName())) {
				Collection<Object> keywords = doc.getFieldValues(SolrFieldDefinition.NEG_KEYWORDS.getName());
				if (keywords != null)
					for (Object obj : keywords) {
						if (obj instanceof String) {
							keywordSet.add((String) obj);
						}
					}
			}

			if (doc.containsKey(SolrFieldDefinition.NEUTRAL_KEYWORDS.getName())) {
				Collection<Object> keywords = doc.getFieldValues(SolrFieldDefinition.NEUTRAL_KEYWORDS.getName());
				if (keywords != null)
					for (Object obj : keywords) {
						if (obj instanceof String) {
							keywordSet.add((String) obj);
						}
					}
			}

			for (String s : keywordSet) {
				result.add(new Tuple2<String, Long>(s, 1L));
			}

			return result;
		}

	}

	public static class ParticipantPairFunction extends FunctionUtils implements PairFunction<SolrDocument, String, Long> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 8274952383527773714L;

		@Override
		public Tuple2<String, Long> call(SolrDocument doc) throws Exception {
			String domain = extractDomainFromDoc(doc);
			String uId = null;

			if (StringUtils.endsWith(domain, ".tw") || StringUtils.endsWith(domain, ".cn") || "zhidao.baidu.com".equals(domain)) {
				return new Tuple2<String, Long>("", 0l);
			}

			if (doc.containsKey(SolrFieldDefinition.USER_ID.getName())) {
				uId = (String) doc.getFieldValue(SolrFieldDefinition.USER_ID.getName());
			}

			if (StringUtils.isBlank(domain) || StringUtils.isBlank(uId)) {
				return new Tuple2<String, Long>("", 0l);
			}

			return new Tuple2<String, Long>(StringUtils.join(domain, "#", uId), 1l);
		}

	}

	public static class CompareByValueLong implements Comparator<Tuple2<String, Long>>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2827629555887247262L;

		@Override
		public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
			return o1._2().compareTo(o2._2());
		}

	}

	/**
	 * 
	 */
	public BuzzAnalyzer(String taskId) throws Exception {
		super();

		this.taskId = taskId;
		jobInput = this.getJobInput(taskId);
		this.profile = jobInput.getProfile();

		appName = "Buzz Analyzer - " + profile.getId();
		sparkConf = newSparkConf();
		context = new JavaSparkContext(sparkConf);
		exceptions = new ArrayList<String>();

	}

	public JavaRDD<SolrDocument> loadAllDocRdd(Date startDate, Date endDate) throws SolrServerException {
		List<String> collectionList = new ArrayList<String>();

		// if (profile.getSourceTypes().contains(SourceType.OTHERS.getSourceTypeId())) {
		collectionList.add(SolrCollection.OTHERS.getValue());
		// }
		if (profile.getSnses() != null && !profile.getSnses().isEmpty()) {
			collectionList.add(SolrCollection.SOCIAL_MEDIA.getValue());
		}

		Set<String> excluded = profile.getExcludeKeywords();

		// For NWD, to be removed later
		// if (profile.getId() == 57) {
		// if (excluded == null) {
		// excluded = new HashSet<String>();
		// }
		// excluded.add("https://www.facebook.com/cosmobodyhk/posts/1471377249833590");
		// excluded.add("https://www.facebook.com/CosmoHK/posts/1025998197451279");
		// excluded.add("https://www.facebook.com/meclubhk/posts/989864761063742");
		// excluded.add("https://www.facebook.com/tao.e.mag/posts/873559552681112");
		// excluded.add("https://www.facebook.com/jmenhk/posts/975361589171655");
		// excluded.add("https://www.facebook.com/beautyexchange.hk/posts/10153549007544720");
		// excluded.add("https://www.facebook.com/Skyposthk/posts/822848017785110");
		// excluded.add("https://www.facebook.com/umagazinehk/posts/10154198785364616");
		// excluded.add("https://www.facebook.com/elleOnlineHK/posts/10153033760101087");
		// excluded.add("https://www.facebook.com/398776703531379/posts/874154442660267");
		// excluded.add("https://www.facebook.com/milkhk/posts/10153624081912269");
		// excluded.add("https://www.facebook.com/mingpaoinews/posts/954370064623129");
		// excluded.add("https://www.facebook.com/shecom/posts/10153627047583185");
		// excluded.add("https://www.facebook.com/YahooStyleHK/posts/774985959290566");
		// excluded.add("https://zh-hk.facebook.com/MagicMissHunny/posts/1187488244610972");
		// excluded.add("https://www.facebook.com/my903/posts/10153278035498558");
		// excluded.add("https://www.facebook.com/nextmagazinefansclub/posts/504536316376983");
		// excluded.add("https://www.facebook.com/Bastillepost/posts/316709395165983");
		// excluded.add("https://www.facebook.com/newmonday.com.hk/posts/10153277712723220");
		// excluded.add("https://www.facebook.com/Bastillepost/posts/316729508497305");
		// excluded.add("https://www.facebook.com/eastweek.com.hk/posts/10152965243867680");
		// excluded.add("https://www.facebook.com/orientalsundayhk/posts/920730107964749");
		// excluded.add("https://www.facebook.com/mingpaoinews/posts/954330887960380");
		// excluded.add("https://www.facebook.com/am730hk/videos/962148067157614/");
		// excluded.add("https://www.facebook.com/MPdeluxe/posts/522338091287803");
		// excluded.add("https://www.facebook.com/OL.Mingpao/posts/810372899079553");
		// excluded.add("https://www.facebook.com/shecom/posts/10153627047583185");
		// excluded.add("https://www.facebook.com/move.magazine.hk/posts/891854954228068");
		// excluded.add("https://www.facebook.com/nowEntLifestyle/posts/978157442248098");
		// excluded.add("https://www.facebook.com/easttouchhk/videos/977362245658348/");
		// excluded.add("https://www.facebook.com/theplaymagazine/posts/906239726121953");
		// excluded.add("https://www.facebook.com/hk.nextmedia/posts/10153759745407448");
		// excluded.add("https://www.facebook.com/mrrmhk/posts/10154266780279041");
		// excluded.add("https://www.facebook.com/shecom/posts/10153629121538185");
		// excluded.add("https://www.facebook.com/YahooStyleHK/posts/775368185919010");
		// excluded.add("https://www.facebook.com/honeynewmonday/posts/10153181712488391");
		// }

		String collections = StringUtils.join(collectionList, ",");
		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), collections);
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), null, startDate, endDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.NEW_SOURCE_TYPE,
						SolrFieldDefinition.NEW_SOURCE_TYPE, SolrFieldDefinition.SOURCE_TYPE, SolrFieldDefinition.SNS_TYPE,
						SolrFieldDefinition.DOMAIN, SolrFieldDefinition.SENTIMENT_SCORE, SolrFieldDefinition.LIKE_COUNT,
						SolrFieldDefinition.SHARE_COUNT, SolrFieldDefinition.COMMENT_COUNT, SolrFieldDefinition.READ_COUNT,
						SolrFieldDefinition.DISLIKE_COUNT }, collections);
		log.info("Query: {}", solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	public JavaRDD<SolrDocument> loadAllDocRddForCustomCalculation(Date startDate, Date endDate) throws SolrServerException {
		List<String> collectionList = new ArrayList<String>();

		// if (profile.getSourceTypes().contains(SourceType.OTHERS.getSourceTypeId())) {
		collectionList.add(SolrCollection.OTHERS.getValue());
		// }
		// if (profile.getSnses() != null && !profile.getSnses().isEmpty()) {
		collectionList.add(SolrCollection.SOCIAL_MEDIA.getValue());
		// }

		Set<String> excluded = profile.getExcludeKeywords();

		// For NWD, to be removed later
		if (profile.getId() == 57) {
			if (excluded == null) {
				excluded = new HashSet<String>();
			}
			excluded.add("https://www.facebook.com/cosmobodyhk/posts/1471377249833590");
			excluded.add("https://www.facebook.com/CosmoHK/posts/1025998197451279");
			excluded.add("https://www.facebook.com/meclubhk/posts/989864761063742");
			excluded.add("https://www.facebook.com/tao.e.mag/posts/873559552681112");
			excluded.add("https://www.facebook.com/jmenhk/posts/975361589171655");
			excluded.add("https://www.facebook.com/beautyexchange.hk/posts/10153549007544720");
			excluded.add("https://www.facebook.com/Skyposthk/posts/822848017785110");
			excluded.add("https://www.facebook.com/umagazinehk/posts/10154198785364616");
			excluded.add("https://www.facebook.com/elleOnlineHK/posts/10153033760101087");
			excluded.add("https://www.facebook.com/398776703531379/posts/874154442660267");
			excluded.add("https://www.facebook.com/milkhk/posts/10153624081912269");
			excluded.add("https://www.facebook.com/mingpaoinews/posts/954370064623129");
			excluded.add("https://www.facebook.com/shecom/posts/10153627047583185");
			excluded.add("https://www.facebook.com/YahooStyleHK/posts/774985959290566");
			excluded.add("https://zh-hk.facebook.com/MagicMissHunny/posts/1187488244610972");
			excluded.add("https://www.facebook.com/my903/posts/10153278035498558");
			excluded.add("https://www.facebook.com/nextmagazinefansclub/posts/504536316376983");
			excluded.add("https://www.facebook.com/Bastillepost/posts/316709395165983");
			excluded.add("https://www.facebook.com/newmonday.com.hk/posts/10153277712723220");
			excluded.add("https://www.facebook.com/Bastillepost/posts/316729508497305");
			excluded.add("https://www.facebook.com/eastweek.com.hk/posts/10152965243867680");
			excluded.add("https://www.facebook.com/orientalsundayhk/posts/920730107964749");
			excluded.add("https://www.facebook.com/mingpaoinews/posts/954330887960380");
			excluded.add("https://www.facebook.com/am730hk/videos/962148067157614/");
			excluded.add("https://www.facebook.com/MPdeluxe/posts/522338091287803");
			excluded.add("https://www.facebook.com/OL.Mingpao/posts/810372899079553");
			excluded.add("https://www.facebook.com/shecom/posts/10153627047583185");
			excluded.add("https://www.facebook.com/move.magazine.hk/posts/891854954228068");
			excluded.add("https://www.facebook.com/nowEntLifestyle/posts/978157442248098");
			excluded.add("https://www.facebook.com/easttouchhk/videos/977362245658348/");
			excluded.add("https://www.facebook.com/theplaymagazine/posts/906239726121953");
			excluded.add("https://www.facebook.com/hk.nextmedia/posts/10153759745407448");
			excluded.add("https://www.facebook.com/mrrmhk/posts/10154266780279041");
			excluded.add("https://www.facebook.com/shecom/posts/10153629121538185");
			excluded.add("https://www.facebook.com/YahooStyleHK/posts/775368185919010");
			excluded.add("https://www.facebook.com/honeynewmonday/posts/10153181712488391");
		}

		String collections = StringUtils.join(collectionList, ",");
		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), collections);
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), null, startDate, endDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.NEW_SOURCE_TYPE,
						SolrFieldDefinition.NEW_SOURCE_TYPE, SolrFieldDefinition.SOURCE_TYPE, SolrFieldDefinition.SNS_TYPE,
						SolrFieldDefinition.DOMAIN, SolrFieldDefinition.SENTIMENT_SCORE, SolrFieldDefinition.LIKE_COUNT,
						SolrFieldDefinition.SHARE_COUNT, SolrFieldDefinition.COMMENT_COUNT, SolrFieldDefinition.READ_COUNT,
						SolrFieldDefinition.DISLIKE_COUNT, SolrFieldDefinition.USER_ID, SolrFieldDefinition.KEYWORDS,
						SolrFieldDefinition.POS_KEYWORDS, SolrFieldDefinition.NEUTRAL_KEYWORDS, SolrFieldDefinition.NEG_KEYWORDS },
				collections);
		log.info("Query: {}", solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	public JavaRDD<SolrDocument> loadRddForPeopleAnalysis(Date startDate, Date endDate) throws SolrServerException {
		String collections = StringUtils.join(new String[] { SolrCollection.SOCIAL_MEDIA.getValue() }, ",");

		Set<String> excluded = profile.getExcludeKeywords();

		// For NWD, to be removed later
		if (profile.getId() == 57) {
			if (excluded == null) {
				excluded = new HashSet<String>();
			}
			excluded.add("https://www.facebook.com/cosmobodyhk/posts/1471377249833590");
			excluded.add("https://www.facebook.com/CosmoHK/posts/1025998197451279");
			excluded.add("https://www.facebook.com/meclubhk/posts/989864761063742");
			excluded.add("https://www.facebook.com/tao.e.mag/posts/873559552681112");
			excluded.add("https://www.facebook.com/jmenhk/posts/975361589171655");
			excluded.add("https://www.facebook.com/beautyexchange.hk/posts/10153549007544720");
			excluded.add("https://www.facebook.com/Skyposthk/posts/822848017785110");
			excluded.add("https://www.facebook.com/umagazinehk/posts/10154198785364616");
			excluded.add("https://www.facebook.com/elleOnlineHK/posts/10153033760101087");
			excluded.add("https://www.facebook.com/398776703531379/posts/874154442660267");
			excluded.add("https://www.facebook.com/milkhk/posts/10153624081912269");
			excluded.add("https://www.facebook.com/mingpaoinews/posts/954370064623129");
			excluded.add("https://www.facebook.com/shecom/posts/10153627047583185");
			excluded.add("https://www.facebook.com/YahooStyleHK/posts/774985959290566");
			excluded.add("https://zh-hk.facebook.com/MagicMissHunny/posts/1187488244610972");
			excluded.add("https://www.facebook.com/my903/posts/10153278035498558");
			excluded.add("https://www.facebook.com/nextmagazinefansclub/posts/504536316376983");
			excluded.add("https://www.facebook.com/Bastillepost/posts/316709395165983");
			excluded.add("https://www.facebook.com/newmonday.com.hk/posts/10153277712723220");
			excluded.add("https://www.facebook.com/Bastillepost/posts/316729508497305");
			excluded.add("https://www.facebook.com/eastweek.com.hk/posts/10152965243867680");
			excluded.add("https://www.facebook.com/orientalsundayhk/posts/920730107964749");
			excluded.add("https://www.facebook.com/mingpaoinews/posts/954330887960380");
			excluded.add("https://www.facebook.com/am730hk/videos/962148067157614/");
			excluded.add("https://www.facebook.com/MPdeluxe/posts/522338091287803");
			excluded.add("https://www.facebook.com/OL.Mingpao/posts/810372899079553");
			excluded.add("https://www.facebook.com/shecom/posts/10153627047583185");
			excluded.add("https://www.facebook.com/move.magazine.hk/posts/891854954228068");
			excluded.add("https://www.facebook.com/nowEntLifestyle/posts/978157442248098");
			excluded.add("https://www.facebook.com/easttouchhk/videos/977362245658348/");
			excluded.add("https://www.facebook.com/theplaymagazine/posts/906239726121953");
			excluded.add("https://www.facebook.com/hk.nextmedia/posts/10153759745407448");
			excluded.add("https://www.facebook.com/mrrmhk/posts/10154266780279041");
			excluded.add("https://www.facebook.com/shecom/posts/10153629121538185");
			excluded.add("https://www.facebook.com/YahooStyleHK/posts/775368185919010");
			excluded.add("https://www.facebook.com/honeynewmonday/posts/10153181712488391");
		}

		final SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), null, startDate, endDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.NEW_SOURCE_TYPE, SolrFieldDefinition.SOURCE_TYPE,
						SolrFieldDefinition.SNS_TYPE, SolrFieldDefinition.DOMAIN, SolrFieldDefinition.USER_ID }, collections);
		log.info("Query: {}", solrQuery.toString());
		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), collections);
		JavaRDD<SolrDocument> docs = solrRDD.queryShards(context, solrQuery);
		return docs;
	}

	public List<BuzzAnalysisData> analyze2() {
		List<BuzzAnalysisData> dataList = new ArrayList<BuzzAnalysisData>();

		Date today = new Date();
		// Date yesterday = DateUtils.getYesterday();

		// if (profile.getId() >= 57 && profile.getId() <= 63) {
		// Date startDate = getEndOfLastYear();
		// for (int i = 0; i < 300; i++) {
		// try {
		// Date processDate = getDay(startDate, (0 - i));
		// BuzzAnalysisData data = null;
		// data = analyze(processDate);
		// if (data != null)
		// dataList.add(data);
		// } catch (Exception e) {
		// log.error(e.getMessage(), e);
		// exceptions.add(e.getMessage());
		// }
		// }
		// } else

		if (jobInput.getDataList().isEmpty()) {
			int max = MAX_RECORD;
			// if (profile.getId() >= 57 && profile.getId() <= 63) {
			// max = 375;
			// }

			for (int i = 0; i < max; i++) {
				try {
					Date processDate = getDay(today, (0 - i));
					BuzzAnalysisData data = null;
					data = analyze(processDate);
					if (data != null)
						dataList.add(data);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
					exceptions.add(e.getMessage());
				}
			}
		} else {
			Map<Date, BuzzAnalysisData> dateRecordMap = new HashMap<Date, BuzzAnalysisData>();
			for (BuzzAnalysisData data : jobInput.getDataList()) {
				dateRecordMap.put(data.getPk().getRecordDate(), data);
			}

			for (int i = 0; i < (0.1 * MAX_RECORD); i++) {
				try {
					Date processDate = getDay(today, (0 - i));
					BuzzAnalysisData data = dateRecordMap.get(processDate);
					// if (data == null || !DateUtils.isToday(data.getUpdateDate())) {
					data = analyze(processDate);
					if (data != null)
						dataList.add(data);
					// }
				} catch (Exception e) {
					log.error(e.getMessage(), e);
					exceptions.add(e.getMessage());
				}
			}
		}

		return dataList;
	}

	public BuzzAnalysisData analyze(Date recordDate) throws Exception {
		log.info("Analyzing buzz profile [{}] for date {}", profile.getName(), recordDate);

		// Date startDate = getStartTime(recordDate);
		// Date endDate = getEndTime(recordDate);
		Date startDate = DateUtils.getStart(recordDate);
		Date endDate = DateUtils.getEnd(recordDate);

		JavaRDD<SolrDocument> allDocs = loadAllDocRdd(startDate, endDate).persist(StorageLevel.MEMORY_AND_DISK());

		/**
		 * All counting with long
		 */
		JavaPairRDD<ResultType, Long> javaPairRdd = allDocs.flatMapToPair(new CountingLong());
		Map<ResultType, Long> resultLong = javaPairRdd.reduceByKeyLocally(new SumLong());

		/**
		 * Calculate sentiment
		 */
		JavaRDD<Double> sentimentRdd = allDocs.map(new SentimentMap());
		double sentimentScore = 0;
		if (!sentimentRdd.isEmpty())
			sentimentScore = sentimentRdd.reduce(new SentimentReduce());

		/**
		 * Coverage
		 */
		JavaPairRDD<String, Long> coveragePair = allDocs.mapToPair(new CoveragePairFunction()).reduceByKey(new SumLong());
		Map<String, Long> coverageResult = coveragePair.collectAsMap();

		allDocs.unpersist();

		JavaRDD<SolrDocument> rdd = loadRddForPeopleAnalysis(startDate, endDate);
		JavaRDD<String> users = rdd.map(new UserCount());
		users = users.distinct();

		long uniqueUserCount = users.count();
		log.info("uniqueUserCount: {}", uniqueUserCount);

		HashMap<ResultType, Object> outMap = new HashMap<ResultType, Object>();
		for (ResultType key : resultLong.keySet()) {
			outMap.put(key, resultLong.get(key));
		}

		PerformanceResult pResult = new PerformanceResult();
		pResult.setUniqueUserCount(uniqueUserCount);
		pResult.setCoverage(coverageResult);
		if (resultLong.containsKey(ResultType.DOCUMENT_COUNT)) {
			pResult.setDocumentCount(resultLong.get(ResultType.DOCUMENT_COUNT));
		} else {
			pResult.setDocumentCount(0);
		}
		if (resultLong.containsKey(ResultType.ENGAGEMENT_SCORE)) {
			pResult.setEngagementScore(resultLong.get(ResultType.ENGAGEMENT_SCORE));
		} else {
			pResult.setEngagementScore(0);
		}

		pResult.setSentimentScore(sentimentScore);

		// if (result.containsKey(ResultType.POSITIVE_SENTIMENT)) {
		// pResult.setPositiveSentiment(result.get(ResultType.POSITIVE_SENTIMENT));
		// } else {
		// pResult.setPositiveSentiment(0);
		// }
		// if (result.containsKey(ResultType.NEGATIVE_SENTIMENT)) {
		// pResult.setNegativeSentiment(result.get(ResultType.NEGATIVE_SENTIMENT));
		// } else {
		// pResult.setNegativeSentiment(0);
		// }
		// if (result.containsKey(ResultType.NEUTRAL_SENTIMENT)) {
		// pResult.setNeutralSentiment(result.get(ResultType.NEUTRAL_SENTIMENT));
		// } else {
		// pResult.setNeutralSentiment(0);
		// }

		// return pResult;
		return toBuzzAnalysisData(recordDate, pResult);
	}

	public BuzzAnalysisData toBuzzAnalysisData(Date recordDate, PerformanceResult pResult) throws Exception {

		BuzzAnalysisData profileData = new BuzzAnalysisData();
		profileData.getPk().setProfileId(profile.getId());
		profileData.setUserId(profile.getUserId());

		profileData.getPk().setRecordDate(recordDate);
		profileData.setUpdateDate(new Date());

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
		cal.setTime(recordDate);

		int yearNumber = cal.getWeekYear();
		int monthNumber = cal.get(Calendar.MONTH) + 1;
		int weekNumber = cal.get(Calendar.WEEK_OF_YEAR);

		String monthStr = yearNumber + (monthNumber < 10 ? "0" + monthNumber : String.valueOf(monthNumber));
		int month = Integer.parseInt(monthStr);
		String weekStr = yearNumber + (weekNumber < 10 ? "0" + weekNumber : String.valueOf(weekNumber));
		int week = Integer.parseInt(weekStr);

		profileData.setMonth(month);
		profileData.setWeek(week);

		// long siteCount = this.getUniqueSiteList().size();
		long siteCount = 1000l;
		log.info("siteCount: {}", siteCount);

		// coverage score
		// Map<String, MutableLong> coverageResult = analysis.getCoverageAnalysis();
		Map<String, Long> coverageResult = pResult.getCoverage();
		log.info("coverageResult: {}", coverageResult);
		long coverageNumber = coverageResult == null ? 0 : coverageResult.keySet().size();

		double coverageIndex = (double) coverageNumber / (Math.max(coverageNumber, siteCount));
		int todayCoverageSocre = (int) (coverageIndex * 100);
		profileData.setCoverageNumber(coverageNumber);
		profileData.setCoverageScore(todayCoverageSocre);

		// engagement
		long engagementValue = pResult.getEngagementScore();

		int engagementScore = 0;
		if (engagementValue != 0) {
			engagementScore = (int) (((double) engagementValue / (engagementValue + 500)) * 100);
		}
		profileData.setEngagementNumber(engagementValue);
		profileData.setEngagementScore(engagementScore);

		// mention
		long mentionValue = pResult.getDocumentCount();
		int mentionScore = 0;
		if (mentionValue != 0) {
			double score = ((double) mentionValue / (mentionValue + 300)) * 100;
			if (score >= 0 && score < 1) {
				mentionScore = 1;
			} else {
				mentionScore = (int) score;
			}
		}
		profileData.setMentionNumber(mentionValue);
		profileData.setVolumeScore(mentionScore);

		// unique people
		// long uniquePeople = analysis.getUniquePeopleValue();
		long uniquePeople = pResult.getUniqueUserCount();
		int peopleScore = 0;
		if (uniquePeople != 0) {
			peopleScore = (int) (((double) uniquePeople / (uniquePeople + 100)) * 100);
		}
		profileData.setPeopleNumber(uniquePeople);
		profileData.setPeopleScore(peopleScore);

		// sentiment
		// long postiveValue = analysis.getPostiveValue();
		// long postiveValue = pResult.getPositiveSentiment();

		// int sentimentScore = 0;
		// if (mentionValue != 0) {
		// double sentimentCoverageScore = ((double) postiveValue / mentionValue) * 100;
		// sentimentScore = (int) (sentimentCoverageScore * 0.6 + sentimentCoverageScore * ((double) mentionScore / 100) * 0.4);
		// }
		// profileData.setSentimentNumber(postiveValue);

		double sentimentScore = pResult.getSentimentScore();
		profileData.setSentimentScore(sentimentScore);
		profileData.setSentimentNumber(0L);

		int smi = (int) (mentionScore * 0.25 + peopleScore * 0.2 + sentimentScore * 0.25 + coverageIndex * 0.3);

		profileData.setAverageSocialMediaIndex(smi);
		profileData.setSocialMediaIndex(smi);

		// analysis = null;
		return profileData;

	}

	public void shutdown() {
		context.stop();
	}

	private Date getDay(Date date, int day) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, day);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	private Date getStartTime(Date date) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 20);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	private Date getEndTime(Date date) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 20);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	public Set<String> getUniqueSiteList() throws Exception {
		return RedisUtil.getInstance().smembers(RedisDefinition.CrawlTaskDef.DOMAIN_SET);
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
	protected String getJobInputHash() {
		return SparkDef.BUZZ_KEYWORD_ANALYSIS_TASK_REQUEST_HASH;
	}

	@Override
	protected Class<BuzzAnalysisJobInput> getJobInputType() {
		return BuzzAnalysisJobInput.class;
	}

	@Override
	protected String getTaskId() {
		return taskId;
	}

	@Override
	protected String getAppName() {
		return appName;
	}

	@Override
	protected Logger getLog() {
		return log;
	}

}

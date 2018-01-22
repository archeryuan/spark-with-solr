/**
 * 
 */
package social.hunt.buzz.spark.sentiment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import social.hunt.buzz.spark.common.SparkProfileAnalyzer;
import social.hunt.buzz.spark.common.function.SumLong;
import social.hunt.buzz.spark.sentiment.comparator.EntityTupleComparator;
import social.hunt.buzz.spark.sentiment.comparator.EntityTuplePairComparator;
import social.hunt.buzz.spark.sentiment.data.SentimentAnalysisJobInput;
import social.hunt.buzz.spark.sentiment.data.SentimentAnalysisJobOutput;
import social.hunt.buzz.spark.sentiment.definition.SentimentDef;
import social.hunt.buzz.spark.sentiment.function.DocSentimentCount;
import social.hunt.buzz.spark.sentiment.function.EntityTypeFilterFunction;
import social.hunt.buzz.spark.sentiment.function.NameEntityCount;
import social.hunt.buzz.spark.sentiment.function.NameEntityCount.EntityType;
import social.hunt.buzz.spark.sentiment.function.SentimentalEntityCount;
import social.hunt.data.domain.DashboardProfile;
import social.hunt.data.domain.SentimentAnalysisCompany;
import social.hunt.data.domain.SentimentAnalysisData;
import social.hunt.data.domain.SentimentAnalysisJobTitle;
import social.hunt.data.domain.SentimentAnalysisKeyword;
import social.hunt.data.domain.SentimentAnalysisPattern;
import social.hunt.data.domain.SentimentAnalysisPerson;
import social.hunt.data.domain.SentimentAnalysisPlace;
import social.hunt.data.domain.SentimentAnalysisProduct;
import social.hunt.data.domain.SentimentAnalysisTheme;
import social.hunt.data.domain.pk.SentimentAnalysisDataPK;
import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.SolrQueryUtil;

import com.google.common.collect.Lists;
import com.lucidworks.spark.SolrRDD;
import com.sa.common.config.CommonConfig;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.redis.definition.RedisDefinition.SparkDef;

/**
 * Fill out the main sentiment calculation logic
 * 
 */
public class SentimentAnalyzer extends SparkProfileAnalyzer<SentimentAnalysisJobInput> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4753506526440298920L;

	private static final Logger log = LoggerFactory.getLogger(SentimentAnalyzer.class);

	private final String taskId;
	private final SentimentAnalysisJobInput jobInput;
	private final String appName;
	private final JavaSparkContext context;
	private final DashboardProfile profile;

	/**
	 * @param taskId
	 * @throws Exception
	 */
	public SentimentAnalyzer(String taskId) throws Exception {
		super();
		this.taskId = taskId;
		this.jobInput = getJobInput(taskId);
		this.appName = "Sentiment Analyzer - " + jobInput.getProfile().getId();
		this.context = new JavaSparkContext(newSparkConf());
		this.profile = jobInput.getProfile();

		log.info("Going to calculate {} day(s) record.", jobInput.getNumDayToAnalyze());
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			log.warn("Input is empty!");
			return;
		}

		/**
		 * args[0] - taskId<BR>
		 * JobInput object in Redis
		 */

		SentimentAnalyzer analyzer = null;
		String taskId = args[0];

		try {
			analyzer = new SentimentAnalyzer(taskId);
		} catch (Exception e2) {
			log.error(e2.getMessage(), e2);
		}

		try {

			List<SentimentAnalysisData> result = analyzer.analyze();
			log.info("{} record(s) in list.", result.size());

			SentimentAnalysisJobOutput output = new SentimentAnalysisJobOutput();
			output.setDataList(result);
			output.setStatus(SentimentAnalysisJobOutput.SUCCESS);
			output.setTaskId(taskId);
			analyzer.submitResult(output);
			log.info("Result submitted successfully.");
		} catch (Exception e) {
			log.error(e.getMessage(), e);

			SentimentAnalysisJobOutput output = new SentimentAnalysisJobOutput();
			output.setStatus(SentimentAnalysisJobOutput.FAIL);
			output.setTaskId(taskId);
			output.setExceptions(Lists.newArrayList(e.getMessage()));
			try {
				analyzer.submitResult(output);
			} catch (Exception e1) {
			}
		}

	}

	protected List<SentimentAnalysisData> analyze() throws SolrServerException {
		log.info("No. of day to calculate: {}", getJobInput().getNumDayToAnalyze());

		List<SentimentAnalysisData> result = new ArrayList<SentimentAnalysisData>();

		for (int i = getJobInput().getNumDayToAnalyze(); i >= 0; i--) {
			Date dateStart, dateEnd;
			dateStart = getNDayBefore(i);
			dateEnd = getDayEnd(dateStart);

			JavaRDD<SolrDocument> rdd = null;
			try {
				rdd = loadAllDocRdd(dateStart, dateEnd).persist(StorageLevel.MEMORY_AND_DISK());

				/**
				 * Things to calculate:<BR>
				 * 1) Daily sentiment score for each profile<BR>
				 * 2) Daily sentiment(positive, negative, neutral) count for each profile<BR>
				 * 3) Daily top 50 keywords & themes & its count for each profile's sentiment(positive, negative, neutral)<BR>
				 * 4) Daily entity & its count for each profile<BR>
				 */
				Map<SentimentDef, Long> sCounts = rdd.flatMapToPair(new DocSentimentCount()).reduceByKey(new SumLong()).collectAsMap();

				BigDecimal pos = null, neg = null, neu = null;

				if (sCounts.get(SentimentDef.POSITIVE) != null) {
					pos = new BigDecimal(sCounts.get(SentimentDef.POSITIVE));
				} else {
					pos = BigDecimal.ZERO;
				}
				if (sCounts.get(SentimentDef.NEGATIVE) != null) {
					neg = new BigDecimal(sCounts.get(SentimentDef.NEGATIVE));
				} else {
					neg = BigDecimal.ZERO;
				}
				if (sCounts.get(SentimentDef.NEUTRAL) != null) {
					neu = new BigDecimal(sCounts.get(SentimentDef.NEUTRAL));
				} else {
					neu = BigDecimal.ZERO;
				}
				sCounts.clear();

				double profileSentimentScore = calculateProfileSentimentScore(pos, neg);

				SentimentAnalysisData data = new SentimentAnalysisData(new SentimentAnalysisDataPK(getProfile().getId(), dateStart));
				data.setKeywords(new HashSet<SentimentAnalysisKeyword>());
				data.setThemes(new HashSet<SentimentAnalysisTheme>());

				data.setDailySentimentScore(profileSentimentScore);
				data.setPosFeedCount(pos.longValue());
				data.setNegFeedCount(neg.longValue());
				data.setNeuFeedCount(neu.longValue());
				data.setUpdateDate(new Date());

				// Map<Pair<EntityType, String>, Long> eCounts = rdd.flatMapToPair(new NameEntityCount()).reduceByKey(new SumLong())
				// .collectAsMap();

				JavaPairRDD<Pair<EntityType, String>, Long> nameEntityRdd = rdd.flatMapToPair(new NameEntityCount())
						.reduceByKey(new SumLong()).cache();

				for (EntityType eType : EntityType.values()) {
					List<Tuple2<Pair<EntityType, String>, Long>> list = nameEntityRdd.filter(new EntityTypeFilterFunction(eType)).top(200,
							new EntityTuplePairComparator());

					if (list != null) {
						for (Tuple2<Pair<EntityType, String>, Long> tuple : list) {

							switch (eType) {
							case COMPANY:
								if (data.getCompanies() == null) {
									data.setCompanies(new HashSet<SentimentAnalysisCompany>());
								}
								data.getCompanies().add(new SentimentAnalysisCompany(tuple._1().getRight(), tuple._2()));
								break;
							case JOB_TITLE:
								if (data.getJobTitles() == null) {
									data.setJobTitles(new HashSet<SentimentAnalysisJobTitle>());
								}
								data.getJobTitles().add(new SentimentAnalysisJobTitle(tuple._1().getRight(), tuple._2()));
								break;
							case PATTERN:
								if (data.getPatterns() == null) {
									data.setPatterns(new HashSet<SentimentAnalysisPattern>());
								}
								data.getPatterns().add(new SentimentAnalysisPattern(tuple._1().getRight(), tuple._2()));
								break;
							case PERSON:
								if (data.getPersons() == null) {
									data.setPersons(new HashSet<SentimentAnalysisPerson>());
								}
								data.getPersons().add(new SentimentAnalysisPerson(tuple._1().getRight(), tuple._2()));
								break;
							case PLACE:
								if (data.getPlaces() == null) {
									data.setPlaces(new HashSet<SentimentAnalysisPlace>());
								}
								data.getPlaces().add(new SentimentAnalysisPlace(tuple._1().getRight(), tuple._2()));
								break;
							case PRODUCT:
								if (data.getProducts() == null) {
									data.setProducts(new HashSet<SentimentAnalysisProduct>());
								}
								data.getProducts().add(new SentimentAnalysisProduct(tuple._1().getRight(), tuple._2()));
								break;
							}
						}
					}
				}
				nameEntityRdd.unpersist();

				for (SentimentDef sentiment : SentimentDef.values()) {
					List<Tuple2<String, Long>> keywordList = rdd
							.flatMapToPair(new SentimentalEntityCount(SentimentalEntityCount.TYPE_KEYWORD, sentiment))
							.reduceByKey(new SumLong()).top(100, new EntityTupleComparator());

					if (keywordList != null) {
						log.info("keywordList size: {}", keywordList.size());
					}

					for (Tuple2<String, Long> tuple : keywordList) {
						data.getKeywords().add(new SentimentAnalysisKeyword(tuple._1(), tuple._2(), sentiment.getSentimentValue()));
					}
					keywordList.clear();

					List<Tuple2<String, Long>> themeList = rdd
							.flatMapToPair(new SentimentalEntityCount(SentimentalEntityCount.TYPE_THEME, sentiment))
							.reduceByKey(new SumLong()).top(100, new EntityTupleComparator());

					if (themeList != null) {
						log.info("themeList size: {}", themeList.size());
					}

					for (Tuple2<String, Long> tuple : themeList) {
						data.getThemes().add(new SentimentAnalysisTheme(tuple._1(), tuple._2(), sentiment.getSentimentValue()));
					}
					themeList.clear();
				}

				result.add(data);
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			} finally {
				if (rdd != null) {
					rdd.unpersist();
				}
			}
		}

		return result;
	}

	private double calculateProfileSentimentScore(BigDecimal positive, BigDecimal negative) {
		BigDecimal result = null;

		if (positive.longValue() < negative.longValue()) {
			if (0 == positive.intValue()) {
				result = new BigDecimal(-10);
			} else {
				result = negative.divide(positive, 5, RoundingMode.HALF_UP).negate();
			}
		} else if (positive.longValue() > negative.longValue()) {
			if (0 == negative.intValue()) {
				result = new BigDecimal(10);
			} else {
				result = positive.divide(negative, 5, RoundingMode.HALF_UP);
			}
		} else {
			result = BigDecimal.ZERO;
		}

		result = result.max(new BigDecimal(-10));
		result = result.min(new BigDecimal(10));

		if (log.isInfoEnabled()) {
			log.info("Positive Count: {}, Negative Count: {}", positive.longValue(), negative.longValue());
			log.info("Profile sentiment score: {}", result.doubleValue());
		}

		return result.doubleValue();
	}

	/**
	 * Get start of the day of N day(s) before
	 * 
	 * @param n
	 * @return
	 */
	protected Date getNDayBefore(int n) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.DAY_OF_YEAR, -1 * n);
		return cal.getTime();
	}

	/**
	 * Get end of the inputed day
	 * 
	 * @param d
	 * @return
	 */
	protected Date getDayEnd(Date d) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Hongkong"), Locale.ENGLISH);
		cal.setTime(d);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.DAY_OF_YEAR, 1);
		cal.add(Calendar.MILLISECOND, -1);
		return cal.getTime();
	}

	protected JavaRDD<SolrDocument> loadAllDocRdd(Date startDate, Date endDate) throws SolrServerException {
		List<String> collectionList = new ArrayList<String>();

		// if (profile.getSourceTypes().contains(SourceType.OTHERS.getSourceTypeId())) {
		collectionList.add(SolrCollection.OTHERS.getValue());
		// }
		if (profile.getSnses() != null && !profile.getSnses().isEmpty()) {
			collectionList.add(SolrCollection.SOCIAL_MEDIA.getValue());
		}

		Set<String> excluded = profile.getExcludeKeywords();

		String collections = StringUtils.join(collectionList, ",");
		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), collections);
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), null, startDate, endDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.TITLE, SolrFieldDefinition.CONTENT, SolrFieldDefinition.URL,
						SolrFieldDefinition.DOMAIN, SolrFieldDefinition.SENTIMENT_SCORE, SolrFieldDefinition.POS_KEYWORDS,
						SolrFieldDefinition.NEG_KEYWORDS, SolrFieldDefinition.NEUTRAL_KEYWORDS, SolrFieldDefinition.POS_THEMES,
						SolrFieldDefinition.NEG_THEMES, SolrFieldDefinition.NEUTRAL_THEMES, SolrFieldDefinition.PERSONS,
						SolrFieldDefinition.PLACES, SolrFieldDefinition.PATTERNS, SolrFieldDefinition.JOB_TITLES,
						SolrFieldDefinition.PRODUCTS, SolrFieldDefinition.COMPANIES }, collections);
		log.info("Query: {}", solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	@Override
	protected String getJobInputHash() {
		return SparkDef.BUZZ_SENTIMENT_ANALYSIS_TASK_REQUEST_HASH;
	}

	@Override
	protected Class<SentimentAnalysisJobInput> getJobInputType() {
		return SentimentAnalysisJobInput.class;
	}

	@Override
	protected String getTaskId() {
		return taskId;
	}

	/**
	 * @return the appName
	 */
	@Override
	protected String getAppName() {
		return appName;
	}

	@Override
	protected Logger getLog() {
		return log;
	}

	/**
	 * @return the jobInput
	 */
	protected SentimentAnalysisJobInput getJobInput() {
		return jobInput;
	}

	/**
	 * @return the context
	 */
	protected JavaSparkContext getContext() {
		return context;
	}

	/**
	 * @return the profile
	 */
	protected DashboardProfile getProfile() {
		return profile;
	}

}

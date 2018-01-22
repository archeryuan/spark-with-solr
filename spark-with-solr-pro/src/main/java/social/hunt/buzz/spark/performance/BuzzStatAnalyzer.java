package social.hunt.buzz.spark.performance;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.buzz.spark.common.SparkProfileAnalyzer;
import social.hunt.buzz.spark.data.MediaDistributionEntity;
import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.buzz.spark.performance.data.BuzzAnalysisJobInput;
import social.hunt.buzz.spark.performance.data.BuzzAnalysisJobOutput;
import social.hunt.buzz.spark.performance.data.PerformanceResult;
import social.hunt.buzz.spark.performance.function.GroupByDate;
import social.hunt.buzz.spark.performance.function.MediaDistributionMap;
import social.hunt.buzz.spark.performance.function.MediaDistributionReduce;
import social.hunt.buzz.spark.performance.function.TopChangelMap;
import social.hunt.buzz.spark.performance.function.TopChannelReduce;
import social.hunt.buzz.spark.performance.function.TopChannelReverseMap;
import social.hunt.common.definition.Sns;
import social.hunt.data.domain.BuzzAnalysisData;
import social.hunt.data.domain.DashboardProfile;
import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.SolrQueryUtil;

import com.lucidworks.spark.SolrRDD;
import com.sa.common.config.CommonConfig;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.DateUtils;
import com.sa.common.util.StringUtils;
import com.sa.common.util.UrlUtil;
import com.sa.redis.definition.RedisDefinition.SparkDef;
import com.sa.redis.util.RedisUtil;

import scala.Tuple2;

public class BuzzStatAnalyzer extends SparkProfileAnalyzer<BuzzAnalysisJobInput> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1253553135143364137L;

	private static final Logger log = LoggerFactory.getLogger(BuzzStatAnalyzer.class);
	private final String appName;
	private final JavaSparkContext context;
	private final DashboardProfile profile;
	private final BuzzAnalysisJobInput jobInput;
	private final String taskId;

	private List<String> exceptions;
	private SparkConf sparkConf;

	private final int NO_OF_DAYS;
	private final Date startDate;
	private final Date endDate;

	/**
	 * @param appName
	 * @param context
	 * @param profile
	 * @param jobInput
	 * @param taskId
	 * @throws Exception
	 */
	public BuzzStatAnalyzer(String taskId) throws Exception {
		super();

		this.taskId = taskId;
		jobInput = this.getJobInput(taskId);
		this.profile = jobInput.getProfile();

		this.startDate = jobInput.getStartDate();
		this.endDate = jobInput.getEndDate();
		this.NO_OF_DAYS = DateUtils.dayDiff(startDate, endDate);

		appName = "Buzz Statistic Analyzer - " + profile.getId() + "-" + NO_OF_DAYS + "days";
		sparkConf = newSparkConf();
		context = new JavaSparkContext(sparkConf);
		exceptions = new ArrayList<String>();
	}

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

		BuzzStatAnalyzer instance = null;

		try {
			String taskId = args[0];

			instance = new BuzzStatAnalyzer(taskId);
			List<BuzzAnalysisData> result = instance.analyze();
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

			List<TopChannelEntity> topChannelList = instance.calTopChannel();
			for (TopChannelEntity entity : topChannelList) {
				String temp = entity.getChannel();
				String[] fields = temp.split("_");
				String channel = fields[1];
				entity.setMedia(Sns.getSns(Short.valueOf(fields[0])).getNameEn());
				if (fields.length > 2) {
					for (int i = 2; i < fields.length; i++) {
						channel = channel + '_' + fields[i];
					}
				}
				entity.setChannel(channel);
			}

			List<MediaDistributionEntity> mediaList = instance.calMediaDistribution();

			output.setMediaDistriList(mediaList);
			output.setTopChannelList(topChannelList);

			instance.submitResult(output);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			instance.submitResult("has exception");
		} finally {
			if (instance != null)
				instance.shutdown();

			// log time spent
			if (log.isInfoEnabled())
				log.info("Time spent in ms: {}", (System.currentTimeMillis() - startMs));
		}
	}

	private List<BuzzAnalysisData> analyze() throws Exception {
		JavaRDD<SolrDocument> allDocs = loadAllDocRdd(startDate, endDate).persist(StorageLevel.MEMORY_AND_DISK());
		// System.out.println("Size of solr docs: " + allDocs.count());

		JavaPairRDD<String, SolrDocument> volumeMapRdd = allDocs.mapToPair(new GroupByDate());
		JavaPairRDD<String, Iterable<SolrDocument>> docMap = volumeMapRdd.groupByKey();
		Map<String, Iterable<SolrDocument>> dateMap = docMap.collectAsMap();

		Map<Date, PerformanceResult> tmpMap = new HashMap<Date, PerformanceResult>();
		List<BuzzAnalysisData> buzzData = new ArrayList<BuzzAnalysisData>();
		for (Map.Entry<String, Iterable<SolrDocument>> entry : dateMap.entrySet()) {
			System.out.println("Date: " + entry.getKey());
			Long engagement = 0L;
			Long people = 0L;

			List<SolrDocument> docListByDate = new ArrayList<SolrDocument>();
			Map<String, Long> coverageList = new HashMap<String, Long>();
			List<String> peopleList = new ArrayList<String>();
			int posCount = 0;
			int negCount = 0;

			PerformanceResult pResult = new PerformanceResult();
			for (SolrDocument solrDoc : entry.getValue()) {
				docListByDate.add(solrDoc);
				String domain = toDomain(solrDoc);
				String uniqueId = toUniqueUser(solrDoc);

				engagement += toEngagementScore(solrDoc);

				if (domain != null) {
					if (!coverageList.containsKey(domain)) {
						coverageList.put(domain, 1L);
					} else {
						Long val = coverageList.get(domain) + 1;
						coverageList.put(domain, val);
					}
				}

				if (null != uniqueId) {
					if (!peopleList.contains(uniqueId)) {
						people += 1;
						peopleList.add(uniqueId);
					}
				}

				Double sScore = toSentimentScore(solrDoc);
				if (sScore != null) {
					if (sScore > 0.22) {
						posCount++;
					} else if (sScore < -0.05) {
						negCount++;
					}
				}
			}
			System.out.println("doc count: " + docListByDate.size() + " engagement: " + engagement + " people:" + people);
			pResult.setDocumentCount(docListByDate.size());
			pResult.setCoverage(coverageList);
			pResult.setEngagementScore(engagement);
			pResult.setUniqueUserCount(people);
			pResult.setSentimentScore(calDailysScore(posCount, negCount));

			String dateStr = entry.getKey();
			SimpleDateFormat df = new SimpleDateFormat("MMddyyyy");
			tmpMap.put(df.parse(dateStr), pResult);
			// buzzData.add(toBuzzAnalysisData(entry.getKey(),
			// tmpMap.get(entry.getKey())));
		}
		allDocs.unpersist();

		for (int i = 0; i <= NO_OF_DAYS; i++) {
			Date recordDate = DateUtils.addDays(startDate, i);
			recordDate = DateUtils.clearTime(recordDate);

			if (!tmpMap.containsKey(recordDate)) {
				PerformanceResult pResult = new PerformanceResult();
				pResult.setCoverage(null);
				pResult.setDocumentCount(0l);
				pResult.setEngagementScore(0l);
				pResult.setSentimentScore(0d);
				pResult.setUniqueUserCount(0l);
				buzzData.add(toBuzzAnalysisData(recordDate, pResult));
			} else {
				System.out.println("Adding not empty data: " + recordDate);
				buzzData.add(toBuzzAnalysisData(recordDate, tmpMap.get(recordDate)));
			}
		}
		System.out.println("size of buzz analysis data: " + buzzData.size());
		return buzzData;
	}

	private List<MediaDistributionEntity> calMediaDistribution() throws SolrServerException {
		JavaRDD<SolrDocument> allDocs = loadMediaDocRdd(startDate, endDate).persist(StorageLevel.MEMORY_AND_DISK());

		JavaPairRDD<String, MediaDistributionEntity> mediaDistriRdd = allDocs.mapToPair(new MediaDistributionMap());
		JavaPairRDD<String, MediaDistributionEntity> mediaDistriReduced = mediaDistriRdd.reduceByKey(new MediaDistributionReduce());
		List<Tuple2<String, MediaDistributionEntity>> mediaTuple2 = mediaDistriReduced.collect();

		List<MediaDistributionEntity> medialist = new ArrayList<MediaDistributionEntity>();
		for (Tuple2<String, MediaDistributionEntity> tuple : mediaTuple2) {
			medialist.add(tuple._2);
		}

		allDocs.unpersist();
		return medialist;
	}

	private List<TopChannelEntity> calTopChannel() throws Exception {
		JavaRDD<SolrDocument> snsDocs = loadSNSDocRdd(startDate, endDate).persist(StorageLevel.MEMORY_AND_DISK());
		JavaPairRDD<String, TopChannelEntity> topChannelRdd = snsDocs.mapToPair(new TopChangelMap());
		JavaPairRDD<String, TopChannelEntity> topChannelRed = topChannelRdd.reduceByKey(new TopChannelReduce());
		JavaPairRDD<Long, TopChannelEntity> topChannelSort = topChannelRed.mapToPair(new TopChannelReverseMap());
		List<Tuple2<Long, TopChannelEntity>> result = topChannelSort.sortByKey(false).take(10);

		List<TopChannelEntity> list = new ArrayList<TopChannelEntity>();
		for (Tuple2<Long, TopChannelEntity> tuple : result) {
			list.add(tuple._2);
		}

		snsDocs.unpersist();
		return list;
	}

	private static Double calDailysScore(int posCount, int negCount) {
		Double sScore = 0d;
		if (posCount < negCount) {
			sScore = -1 * ((double) negCount / (double) posCount);
		} else if (posCount > negCount) {
			sScore = ((double) posCount / (double) negCount);
		} else {
			sScore = 0d;
		}
		if (sScore < -10)
			sScore = -10d;
		if (sScore > 10)
			sScore = 10d;
		return sScore;
	}

	private static Double toSentimentScore(SolrDocument doc) {
		Double sentimentScore = null;
		Object sentimentObj = doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
		if (null != sentimentObj) {
			sentimentScore = Double.parseDouble(sentimentObj.toString());
		}
		return sentimentScore;
	}

	private static Long toEngagementScore(SolrDocument doc) {
		Long score = new Long(0);

		if (doc.containsKey(SolrFieldDefinition.LIKE_COUNT.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
		}

		if (doc.containsKey(SolrFieldDefinition.READ_COUNT.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
		}

		if (doc.containsKey(SolrFieldDefinition.SHARE_COUNT.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
		}

		if (doc.containsKey(SolrFieldDefinition.COMMENT_COUNT.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
		}

		if (doc.containsKey(SolrFieldDefinition.DISLIKE_COUNT.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.DISLIKE_COUNT.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_ANGRY.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.FB_ANGRY.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_LOVE.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.FB_LOVE.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_HAHA.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.FB_HAHA.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_SAD.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.FB_SAD.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_THANKFUL.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.FB_THANKFUL.getName());
		}
		if (doc.containsKey(SolrFieldDefinition.FB_WOW.getName())) {
			score += (long) doc.getFieldValue(SolrFieldDefinition.FB_WOW.getName());
		}

		return score;
	}

	private static String toDomain(SolrDocument doc) {
		if (doc.containsKey(SolrFieldDefinition.DOMAIN.getName())) {
			String tmp = (String) doc.getFieldValue(SolrFieldDefinition.DOMAIN.getName());
			if (!StringUtils.isBlank(tmp))
				return tmp;
		}

		if (doc.containsKey(SolrFieldDefinition.SNS_TYPE.getName())) {
			Sns sns = Sns.getSns((int) doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName()));
			String domain = null;
			switch (sns) {
			case FACEBOOK:
				domain = "facebook.com";
				break;
			case INSTAGRAM:
				domain = "instagram.com";
				break;
			case TWITTER:
				domain = "twitter.com";
				break;
			case WEIBO:
				domain = "weibo.com";
				break;
			// case WEIXIN:
			// domain = "qq.com";
			// break;
			case YOUTUBE:
				domain = "youtube.com";
				break;
			}

			if (!StringUtils.isBlank(domain))
				return domain;
		}

		String domain = StringUtils.defaultString(UrlUtil.extractDomain((String) doc.getFieldValue(SolrFieldDefinition.URL.getName())));
		return domain;
	}

	private static String toUniqueUser(SolrDocument doc) {
		String userId = null;
		if (doc.containsKey(SolrFieldDefinition.USER_ID.getName()))
			userId = (String) doc.getFieldValue(SolrFieldDefinition.USER_ID.getName());
		return userId;
	}

	private JavaRDD<SolrDocument> loadMediaDocRdd(Date startDate, Date endDate) throws SolrServerException {
		Set<String> excluded = profile.getExcludeKeywords();

		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), SolrCollection.getAllCollectionString());
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), null, startDate, endDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.SNS_TYPE, SolrFieldDefinition.NEW_SOURCE_TYPE,
						SolrFieldDefinition.PUBLISH_DATE }, SolrCollection.getAllCollectionString());
		System.out.println("Query: " + solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	private JavaRDD<SolrDocument> loadAllDocRdd(Date startDate, Date endDate) throws SolrServerException {
		Set<String> excluded = profile.getExcludeKeywords();

		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), SolrCollection.getAllCollectionString());
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), null, startDate, endDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.SNS_TYPE, SolrFieldDefinition.DOMAIN,
						SolrFieldDefinition.SENTIMENT_SCORE, SolrFieldDefinition.LIKE_COUNT, SolrFieldDefinition.SHARE_COUNT,
						SolrFieldDefinition.COMMENT_COUNT, SolrFieldDefinition.READ_COUNT, SolrFieldDefinition.DISLIKE_COUNT,
						SolrFieldDefinition.USER_ID, SolrFieldDefinition.PUBLISH_DATE, SolrFieldDefinition.FB_ANGRY,
						SolrFieldDefinition.FB_HAHA, SolrFieldDefinition.FB_LOVE, SolrFieldDefinition.FB_SAD,
						SolrFieldDefinition.FB_THANKFUL, SolrFieldDefinition.FB_WOW }, SolrCollection.getAllCollectionString());
		System.out.println("Query: " + solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	private JavaRDD<SolrDocument> loadSNSDocRdd(Date startDate, Date endDate) throws SolrServerException {
		Set<String> excluded = profile.getExcludeKeywords();

		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), "social-media");
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), null, startDate, endDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.SNS_TYPE, SolrFieldDefinition.POS_THEMES, SolrFieldDefinition.NEG_THEMES,
						SolrFieldDefinition.NEUTRAL_THEMES, SolrFieldDefinition.LIKE_COUNT, SolrFieldDefinition.SHARE_COUNT,
						SolrFieldDefinition.COMMENT_COUNT, SolrFieldDefinition.READ_COUNT, SolrFieldDefinition.USER_NAME }, "social-media");
		System.out.println("Query: " + solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	private BuzzAnalysisData toBuzzAnalysisData(Date recordDate, PerformanceResult pResult) throws Exception {

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
		// Map<String, MutableLong> coverageResult =
		// analysis.getCoverageAnalysis();
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
		// double sentimentCoverageScore = ((double) postiveValue /
		// mentionValue) * 100;
		// sentimentScore = (int) (sentimentCoverageScore * 0.6 +
		// sentimentCoverageScore * ((double) mentionScore / 100) * 0.4);
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

	public void shutdown() {
		context.stop();
	}

	@Override
	protected String getJobInputHash() {
		return SparkDef.BUZZ_STATISTICS_ANALYSIS_TASK_REQUEST_HASH;
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

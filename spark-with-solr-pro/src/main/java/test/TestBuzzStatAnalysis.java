package test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.buzz.spark.performance.data.PerformanceResult;
import social.hunt.buzz.spark.performance.function.GroupByDate;
import social.hunt.common.definition.Sns;
import social.hunt.data.domain.BuzzAnalysisData;
import social.hunt.solr.definition.SolrCollection;

import com.google.gson.Gson;
import com.lucidworks.spark.SolrRDD;
import com.sa.common.config.CommonConfig;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.DateUtils;

public class TestBuzzStatAnalysis {

	private static final Logger log = LoggerFactory.getLogger(TestBuzzStatAnalysis.class);

	public static void main(String[] args) throws Exception {
		Gson gson = new Gson();

		SparkConf sparkConf = new SparkConf().setAppName("TestBuzzStatAnalyser");
		sparkConf.setMaster("spark://solr-node2:7077");
		String[] jars = new String[1];
		jars[0] = "target/buzz-statistics-spark-0.6.3-SNAPSHOT-jar-with-dependencies.jar";
		sparkConf.setJars(jars);
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<SolrDocument> allDocs = loadAllDocRdd(new Date(), DateUtils.getNDaysBefore(90), ctx)
				.persist(StorageLevel.MEMORY_AND_DISK());

		System.out.println("Size of solr docs: " + allDocs.count());

		JavaPairRDD<String, SolrDocument> volumeMapRdd = allDocs.mapToPair(new GroupByDate());
		System.out.println("Size of volumeMapRdd: " + volumeMapRdd.count());

		JavaPairRDD<String, Iterable<SolrDocument>> docMap = volumeMapRdd.groupByKey();
		System.out.println("size of docMap: " + docMap.count());
		Map<String, Iterable<SolrDocument>> dateMap = docMap.collectAsMap();
		System.out.println("Size of date map: " + dateMap.size());

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
			DateFormat df = new SimpleDateFormat("MMddyyyy");
			tmpMap.put(df.parse(dateStr), pResult);
		}
		allDocs.unpersist();

		for (int i = 0; i < 365; i++) {
			Date recordDate = DateUtils.getNDaysBefore(i);
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
				buzzData.add(toBuzzAnalysisData(recordDate, tmpMap.get(recordDate)));
			}
		}

		// log.info("results: {}", gson.toJson(buzzData));
		ctx.stop();
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

	public static JavaRDD<SolrDocument> loadAllDocRdd(Date startDate, Date endDate, JavaSparkContext context) throws SolrServerException {
		List<String> collectionList = new ArrayList<String>();

		collectionList.add(SolrCollection.OTHERS.getValue());
		collectionList.add(SolrCollection.SOCIAL_MEDIA.getValue());
		String collections = StringUtils.join(collectionList, ",");

		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), collections);
		SolrQuery solrQ = new SolrQuery();
		solrQ.setQuery("*:*");
		// solrQ.setQuery("*:*");
		solrQ.addSort(SolrFieldDefinition.URL.getName(), ORDER.asc);
		solrQ.addSort(SolrFieldDefinition.PUBLISH_DATE.getName(), ORDER.desc);
		solrQ.add("collection", StringUtils.join(collections, ","));

		// SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
		// profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), null, startDate, endDate, null,
		// new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.NEW_SOURCE_TYPE,
		// SolrFieldDefinition.NEW_SOURCE_TYPE, SolrFieldDefinition.SOURCE_TYPE, SolrFieldDefinition.SNS_TYPE,
		// SolrFieldDefinition.DOMAIN, SolrFieldDefinition.SENTIMENT_SCORE, SolrFieldDefinition.LIKE_COUNT,
		// SolrFieldDefinition.SHARE_COUNT, SolrFieldDefinition.COMMENT_COUNT, SolrFieldDefinition.READ_COUNT,
		// SolrFieldDefinition.DISLIKE_COUNT }, collections);
		System.out.println("Query: " + solrQ.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQ);
		return allDocs;
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
		return score;
	}

	private static String toDomain(SolrDocument doc) {
		String domain = null;
		if (doc.containsKey(SolrFieldDefinition.DOMAIN.getName())) {
			domain = (String) doc.getFieldValue(SolrFieldDefinition.DOMAIN.getName());

		} else if (doc.containsKey(SolrFieldDefinition.SNS_TYPE.getName())) {
			Sns sns = Sns.getSns((int) doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName()));
			domain = null;
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
			case YOUTUBE:
				domain = "youtube.com";
				break;
			}
		}
		return domain;
	}

	private static Double toSentimentScore(SolrDocument doc) {
		Double sentimentScore = null;
		Object sentimentObj = doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
		if (null != sentimentObj) {
			sentimentScore = Double.parseDouble(sentimentObj.toString());
		}
		return sentimentScore;
	}

	private static String toUniqueUser(SolrDocument doc) {
		String userId = null;
		if (doc.containsKey(SolrFieldDefinition.USER_ID.getName()))
			userId = (String) doc.getFieldValue(SolrFieldDefinition.USER_ID.getName());
		return userId;
	}

	private static BuzzAnalysisData toBuzzAnalysisData(Date recordDate, PerformanceResult pResult) throws Exception {

		BuzzAnalysisData profileData = new BuzzAnalysisData();

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

		// coverage score
		// Map<String, MutableLong> coverageResult = analysis.getCoverageAnalysis();
		Map<String, Long> coverageResult = pResult.getCoverage();

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
}

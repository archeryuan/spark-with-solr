package social.hunt.buzz.spark.performance;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import org.hibernate.loader.custom.Return;
import org.jruby.RubyProcess.Sys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.buzz.spark.common.SparkProfileAnalyzer;
import social.hunt.buzz.spark.data.NewTopChannel;
import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.buzz.spark.data.TopWebsiteEntity;
import social.hunt.buzz.spark.data.TrendEntity;
import social.hunt.buzz.spark.performance.data.BuzzAnalysisJobInput;
import social.hunt.buzz.spark.performance.data.BuzzAnalysisJobOutput;
import social.hunt.buzz.spark.performance.data.PerformanceResult;
import social.hunt.buzz.spark.performance.function.GroupByDate;
import social.hunt.buzz.spark.performance.function.NewTopChangelMap;
import social.hunt.buzz.spark.performance.function.NewTopChannelReduce;
import social.hunt.buzz.spark.performance.function.TopChangelCommentNegMap;
import social.hunt.buzz.spark.performance.function.TopChangelCommentNeuMap;
import social.hunt.buzz.spark.performance.function.TopChangelCommentPosMap;
import social.hunt.buzz.spark.performance.function.TopChangelCommentTotalMap;
import social.hunt.buzz.spark.performance.function.TopChangelFacebookFilter;
import social.hunt.buzz.spark.performance.function.TopChangelInstagramFilter;
import social.hunt.buzz.spark.performance.function.TopChangelLineqFilter;
import social.hunt.buzz.spark.performance.function.TopChangelMap;
import social.hunt.buzz.spark.performance.function.TopChangelNegMap;
import social.hunt.buzz.spark.performance.function.TopChangelNeuMap;
import social.hunt.buzz.spark.performance.function.TopChangelPlurkFilter;
import social.hunt.buzz.spark.performance.function.TopChangelPosMap;
import social.hunt.buzz.spark.performance.function.TopChangelTotalMap;
import social.hunt.buzz.spark.performance.function.TopChangelTwitterFilter;
import social.hunt.buzz.spark.performance.function.TopChangelWeiboFilter;
import social.hunt.buzz.spark.performance.function.TopChangelWeixinFilter;
import social.hunt.buzz.spark.performance.function.TopChangelYoutubeFilter;
import social.hunt.buzz.spark.performance.function.TopChannelReduce;
import social.hunt.buzz.spark.performance.function.TopChannelReverseMap;
import social.hunt.buzz.spark.performance.function.TopWebsiteCommentNegMap;
import social.hunt.buzz.spark.performance.function.TopWebsiteCommentNeuMap;
import social.hunt.buzz.spark.performance.function.TopWebsiteCommentPosMap;
import social.hunt.buzz.spark.performance.function.TopWebsiteCommentTotalMap;
import social.hunt.buzz.spark.performance.function.TopWebsiteMap;
import social.hunt.buzz.spark.performance.function.TopWebsiteNegMap;
import social.hunt.buzz.spark.performance.function.TopWebsiteNeuMap;
import social.hunt.buzz.spark.performance.function.TopWebsitePosMap;
import social.hunt.buzz.spark.performance.function.TopWebsiteReduce;
import social.hunt.buzz.spark.performance.function.TopWebsiteTotalMap;
import social.hunt.buzz.spark.performance.function.TrendMap;
import social.hunt.buzz.spark.performance.function.TrendReduce;
import social.hunt.common.definition.Sns;
import social.hunt.data.domain.BuzzAnalysisData;
import social.hunt.data.domain.DashboardProfile;
import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.SolrQueryUtil;

import com.google.gson.Gson;
import com.lucidworks.spark.SolrRDD;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.sa.common.config.CommonConfig;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.json.JsonUtil;
import com.sa.common.util.DateUtils;
import com.sa.common.util.StringUtils;
import com.sa.common.util.UrlUtil;
import com.sa.redis.definition.RedisDefinition.SparkDef;
import com.sa.redis.util.RedisUtil;

import scala.Tuple2;

public class BuzzFilterStatistic extends SparkProfileAnalyzer<BuzzAnalysisJobInput> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1253553135143364137L;

	private static final Logger log = LoggerFactory.getLogger(BuzzFilterStatistic.class);
	private final String appName;
	private final JavaSparkContext context;
	private final DashboardProfile profile;
	private final BuzzAnalysisJobInput jobInput;
	private final String taskId;
	private static Gson gson;

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
	public BuzzFilterStatistic(String taskId) throws Exception {
		super();

		this.taskId = taskId;
		jobInput = this.getJobInput(taskId);
		this.profile = jobInput.getProfile();

		this.startDate = jobInput.getStartDate();
		this.endDate = jobInput.getEndDate();
		this.NO_OF_DAYS = DateUtils.dayDiff(startDate, endDate);

		appName = "Buzz Filter Statistic - " + profile.getId();
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

		BuzzFilterStatistic instance = null;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		MongoCredential credential = MongoCredential.createCredential(CommonConfig.getInstance().getMongoDBUser(), "admin", CommonConfig
				.getInstance().getMongoDBPassword().toCharArray());
		MongoClient mongoClient = new MongoClient(new ServerAddress(CommonConfig.getInstance().getMongoDBHost(), CommonConfig.getInstance()
				.getMongoDBPort()), Arrays.asList(credential));

		MongoDatabase trend = mongoClient.getDatabase("trend");
		MongoDatabase statistic = mongoClient.getDatabase("statistic");
		MongoCollection<Document> topChannel = statistic.getCollection("newTopChannel");
		MongoCollection<Document> topWebsite = statistic.getCollection("topWebsite");

		MongoDatabase trendTargeted = mongoClient.getDatabase("trend_targeted");
		MongoDatabase statisticTargeted = mongoClient.getDatabase("statistic_targeted");
		MongoCollection<Document> topChannelTargeted = statisticTargeted.getCollection("newTopChannel_targeted");
		MongoCollection<Document> topWebsiteTargeted = statisticTargeted.getCollection("topWebsite_targeted");

		try {
			String taskId = args[0];
			instance = new BuzzFilterStatistic(taskId);

			MongoCollection<Document> trendColl = trend.getCollection("profile_" + instance.getProfile().getId());
			MongoCollection<Document> trendCollTargeted = trendTargeted.getCollection("profile_" + instance.getProfile().getId());

			UpdateOptions options = new UpdateOptions().upsert(true);

			System.out.println("start calculate -----------------------------------------------------" + instance.getProfile().getId());

			Date s365Date = DateUtils.getNDaysBefore(365);
			s365Date = DateUtils.clearTime(s365Date);

			Date s90Date = DateUtils.getNDaysBefore(90);
			s90Date = DateUtils.clearTime(s90Date);

			Date s30Date = DateUtils.getNDaysBefore(30);
			s30Date = DateUtils.clearTime(s30Date);

			Date s7Date = DateUtils.getNDaysBefore(7);
			s7Date = DateUtils.clearTime(s7Date);

			Date s1Date = DateUtils.getNDaysBefore(1);
			s1Date = DateUtils.clearTime(s1Date);

			Date eDate = new Date();
			
			try{

			List<TrendEntity> trendRes = instance.trendStatistic(s30Date, eDate, false);
			for (TrendEntity entity : trendRes) {
				String jsonStr = gsonUtil().toJson(entity);
				Document doc = Document.parse(jsonStr);
				System.out.println("entity.getDateStr()=========================" + entity.getDateStr());
				doc.append("_id", entity.getDateStr()).append("date", df.parse(entity.getDateStr()));
				trendColl.updateOne(new Document("_id", entity.getDateStr()), new Document("$set", doc), options);
			}
			}catch(Exception e){
				log.error(e.getMessage());
			}

//			try{
//			Map<String, Map<String, List<NewTopChannel>>> topChannelRes = instance.topChannelStatistic(s365Date, eDate, false);
//			Document topChannelDoc = Document.parse(gsonUtil().toJson(topChannelRes));
//			Document channelDoc = new Document("_id", instance.getProfile().getId()).append("1y", topChannelDoc);
//			topChannel.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc), options);
//			}catch(Exception e){
//				log.error(e.getMessage());
//			}

//			try{
//			Map<String, List<TopWebsiteEntity>> topWebsiteRes = instance.topWebsiteStatistic(s365Date, eDate, false);
//			Document topWebsiteDoc = Document.parse(gsonUtil().toJson(topWebsiteRes));
//			Document websiteDoc = new Document("_id", instance.getProfile().getId()).append("1y", topWebsiteDoc);
//			topWebsite.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc), options);
//			}catch(Exception e){
//				log.error(e.getMessage());
//			}
			
			// ----------------------------------------caculate 90 days
			// ----------------------
//			try{
//			Map<String, Map<String, List<NewTopChannel>>> topChannelRes90 = instance.topChannelStatistic(s90Date, eDate, false);
//			Document topChannelDoc90 = Document.parse(gsonUtil().toJson(topChannelRes90));
//			Document channelDoc90 = new Document("_id", instance.getProfile().getId()).append("3m", topChannelDoc90);
//			topChannel.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc90), options);
//			}catch(Exception e){
//				log.error(e.getMessage());
//			}

//			try{
//			Map<String, List<TopWebsiteEntity>> topWebsiteRes90 = instance.topWebsiteStatistic(s90Date, eDate, false);
//			Document topWebsiteDoc90 = Document.parse(gsonUtil().toJson(topWebsiteRes90));
//			Document websiteDoc90 = new Document("_id", instance.getProfile().getId()).append("3m", topWebsiteDoc90);
//			topWebsite.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc90), options);
//			}catch(Exception e){
//				log.error(e.getMessage());
//			}

			// ----------------------------------------caculate 30 days
			// ----------------------
			try{
			Map<String, Map<String, List<NewTopChannel>>> topChannelRes30 = instance.topChannelStatistic(s30Date, eDate, false);
			Document topChannelDoc30 = Document.parse(gsonUtil().toJson(topChannelRes30));
			Document channelDoc30 = new Document("_id", instance.getProfile().getId()).append("1m", topChannelDoc30);
			topChannel.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc30), options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			try{
			Map<String, List<TopWebsiteEntity>> topWebsiteRes30 = instance.topWebsiteStatistic(s30Date, eDate, false);
			Document topWebsiteDoc30 = Document.parse(gsonUtil().toJson(topWebsiteRes30));
			Document websiteDoc30 = new Document("_id", instance.getProfile().getId()).append("1m", topWebsiteDoc30);
			topWebsite.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc30), options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			// ----------------------------------------caculate 7 days
			// ----------------------
			try{
			Map<String, Map<String, List<NewTopChannel>>> topChannelRes7 = instance.topChannelStatistic(s7Date, eDate, false);
			Document topChannelDoc7 = Document.parse(gsonUtil().toJson(topChannelRes7));
			Document channelDoc7 = new Document("_id", instance.getProfile().getId()).append("7d", topChannelDoc7);
			topChannel.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc7), options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			try{
			Map<String, List<TopWebsiteEntity>> topWebsiteRes7 = instance.topWebsiteStatistic(s7Date, eDate, false);
			Document topWebsiteDoc7 = Document.parse(gsonUtil().toJson(topWebsiteRes7));
			Document websiteDoc7 = new Document("_id", instance.getProfile().getId()).append("7d", topWebsiteDoc7);
			topWebsite.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc7), options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			// ----------------------------------------caculate 1 day
			// ----------------------
			try{
			Map<String, Map<String, List<NewTopChannel>>> topChannelRes1 = instance.topChannelStatistic(s1Date, eDate, false);
			Document topChannelDoc1 = Document.parse(gsonUtil().toJson(topChannelRes1));
			Document channelDoc1 = new Document("_id", instance.getProfile().getId()).append("1d", topChannelDoc1);
			topChannel.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc1), options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			try{
			Map<String, List<TopWebsiteEntity>> topWebsiteRes1 = instance.topWebsiteStatistic(s1Date, eDate, false);
			Document topWebsiteDoc1 = Document.parse(gsonUtil().toJson(topWebsiteRes1));
			Document websiteDoc1 = new Document("_id", instance.getProfile().getId()).append("1d", topWebsiteDoc1);
			topWebsite.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc1), options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			/**
			 * Targeted Sites Calculations
			 */
			try{
			List<TrendEntity> trendResTargeted = instance.trendStatistic(s365Date, eDate, true);
			for (TrendEntity entity : trendResTargeted) {
				String jsonStr = gsonUtil().toJson(entity);
				Document doc = Document.parse(jsonStr);
				doc.append("_id", entity.getDateStr()).append("date", df.parse(entity.getDateStr()));
				trendCollTargeted.updateOne(new Document("_id", entity.getDateStr()), new Document("$set", doc), options);
			}
			}catch(Exception e){
				log.error(e.getMessage());
			}
			
			try{
			Map<String, Map<String, List<NewTopChannel>>> topChannelResTargeted = instance.topChannelStatistic(s365Date, eDate, true);
			Document topChannelDocTargeted = Document.parse(gsonUtil().toJson(topChannelResTargeted));
			Document channelDocTargeted = new Document("_id", instance.getProfile().getId()).append("1y", topChannelDocTargeted);
			topChannelTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDocTargeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			try{
			Map<String, List<TopWebsiteEntity>> topWebsiteResTargeted = instance.topWebsiteStatistic(s365Date, eDate, true);
			Document topWebsiteDocTargeted = Document.parse(gsonUtil().toJson(topWebsiteResTargeted));
			Document websiteDocTargeted = new Document("_id", instance.getProfile().getId()).append("1y", topWebsiteDocTargeted);
			topWebsiteTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDocTargeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			// ----------------------------------------caculate 90 days
			// ----------------------
			try{
			Map<String, Map<String, List<NewTopChannel>>> topChannelRes90Targeted = instance.topChannelStatistic(s90Date, eDate, true);
			Document topChannelDoc90Targeted = Document.parse(gsonUtil().toJson(topChannelRes90Targeted));
			Document channelDoc90Targeted = new Document("_id", instance.getProfile().getId()).append("3m", topChannelDoc90Targeted);
			topChannelTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc90Targeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			try{
			Map<String, List<TopWebsiteEntity>> topWebsiteRes90Targeted = instance.topWebsiteStatistic(s90Date, eDate, true);
			Document topWebsiteDoc90Targeted = Document.parse(gsonUtil().toJson(topWebsiteRes90Targeted));
			Document websiteDoc90Targeted = new Document("_id", instance.getProfile().getId()).append("3m", topWebsiteDoc90Targeted);
			topWebsiteTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc90Targeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			// ----------------------------------------caculate 30 days
			// ----------------------
			try{
			Map<String, Map<String, List<NewTopChannel>>> topChannelRes30Targeted = instance.topChannelStatistic(s30Date, eDate, true);
			Document topChannelDoc30Targeted = Document.parse(gsonUtil().toJson(topChannelRes30Targeted));
			Document channelDoc30Targeted = new Document("_id", instance.getProfile().getId()).append("1m", topChannelDoc30Targeted);
			topChannelTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc30Targeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			try{
			Map<String, List<TopWebsiteEntity>> topWebsiteRes30Targeted = instance.topWebsiteStatistic(s30Date, eDate, true);
			Document topWebsiteDoc30Targeted = Document.parse(gsonUtil().toJson(topWebsiteRes30Targeted));
			Document websiteDoc30Targeted = new Document("_id", instance.getProfile().getId()).append("1m", topWebsiteDoc30Targeted);
			topWebsiteTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc30Targeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			// ----------------------------------------caculate 7 days
			// ----------------------
			try{
			Map<String, Map<String, List<NewTopChannel>>> topChannelRes7Targeted = instance.topChannelStatistic(s7Date, eDate, true);
			Document topChannelDoc7Targeted = Document.parse(gsonUtil().toJson(topChannelRes7Targeted));
			Document channelDoc7Targeted = new Document("_id", instance.getProfile().getId()).append("7d", topChannelDoc7Targeted);
			topChannelTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc7Targeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			try{
			Map<String, List<TopWebsiteEntity>> topWebsiteRes7Targeted = instance.topWebsiteStatistic(s7Date, eDate, true);
			Document topWebsiteDoc7Targeted = Document.parse(gsonUtil().toJson(topWebsiteRes7Targeted));
			Document websiteDoc7Targeted = new Document("_id", instance.getProfile().getId()).append("7d", topWebsiteDoc7Targeted);
			topWebsiteTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc7Targeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			// ----------------------------------------caculate 1 day
			// ----------------------
			try{
			Map<String, Map<String, List<NewTopChannel>>> topChannelRes1Targeted = instance.topChannelStatistic(s1Date, eDate, true);
			Document topChannelDoc1Targeted = Document.parse(gsonUtil().toJson(topChannelRes1Targeted));
			Document channelDoc1Targeted = new Document("_id", instance.getProfile().getId()).append("1d", topChannelDoc1Targeted);
			topChannelTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", channelDoc1Targeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			try{
			Map<String, List<TopWebsiteEntity>> topWebsiteRes1Targeted = instance.topWebsiteStatistic(s1Date, eDate, true);
			Document topWebsiteDoc1Targeted = Document.parse(gsonUtil().toJson(topWebsiteRes1Targeted));
			Document websiteDoc1Targeted = new Document("_id", instance.getProfile().getId()).append("1d", topWebsiteDoc1Targeted);
			topWebsiteTargeted.updateOne(new Document("_id", instance.getProfile().getId()), new Document("$set", websiteDoc1Targeted),
					options);
			}catch(Exception e){
				log.error(e.getMessage());
			}

			instance.submitResult("finish buzz filter statistic for profile:" + instance.getProfile().getId());

		} catch (Exception e) {
			e.printStackTrace();
			instance.submitResult("Fail buzz filter statistic for profile:" + instance.getProfile().getId() + "    Exception:"
					+ e.getMessage());
			// throw e;
		} finally {
			if (instance != null)
				instance.shutdown();

			mongoClient.close();

			// log time spent
			if (log.isInfoEnabled())
				log.info("Time spent in ms: {}", (System.currentTimeMillis() - startMs));
		}
	}

	private List<TrendEntity> trendStatistic(Date sDate, Date eDate, boolean isTargeted) throws Exception {
		JavaRDD<SolrDocument> allDocs = loadAllDocRdd(sDate, eDate, isTargeted);
		List<TrendEntity> trendList = new ArrayList<TrendEntity>();
		if (allDocs != null) {
			allDocs.persist(StorageLevel.MEMORY_AND_DISK());
			System.out.println("start allDocs -----------------------------------------------------" + allDocs.count());

			JavaPairRDD<String, TrendEntity> trendMapRdd = allDocs.mapToPair(new TrendMap());
			JavaPairRDD<String, TrendEntity> trendRed = trendMapRdd.reduceByKey(new TrendReduce());
			Map<String, TrendEntity> dataMap = trendRed.collectAsMap();

			for (TrendEntity entity : dataMap.values()) {
				entity.setDomainNum(entity.getDomain().size());
				entity.setDomain(null);
				entity.setDomainNegNum(entity.getDomainNeg().size());
				entity.setDomainNeg(null);
				entity.setDomainPosNum(entity.getDomainPos().size());
				entity.setDomainPos(null);
				entity.setDomainNeuNum(entity.getDomainNeu().size());
				entity.setDomainNeu(null);

				entity.setPeopleNum(entity.getPeople().size());
				entity.setPeople(null);
				entity.setPeopleNegNum(entity.getPeopleNeg().size());
				entity.setPeopleNeg(null);
				entity.setPeoplePosNum(entity.getPeoplePos().size());
				entity.setPeoplePos(null);
				entity.setPeopleNeuNum(entity.getPeopleNeu().size());
				entity.setPeopleNeu(null);
				trendList.add(entity);
			}
			allDocs.unpersist();
		}

		return trendList;
	}

	private Map<String, Map<String, List<NewTopChannel>>> topChannelStatistic(Date sDate, Date eDate, boolean isTargeted) throws Exception {
		Map<String, Map<String, List<NewTopChannel>>> result = new HashMap<String, Map<String, List<NewTopChannel>>>();
		JavaRDD<SolrDocument> snsDocs = loadSNSDocRdd(sDate, eDate, isTargeted);
		System.out.println("top channel all docs number:"+snsDocs.count());
		System.out.println("start top channel statistic xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

		if (snsDocs != null) {
			snsDocs.persist(StorageLevel.MEMORY_AND_DISK());
			JavaPairRDD<String, NewTopChannel> channelMap = snsDocs.mapToPair(new NewTopChangelMap());
			JavaPairRDD<String, NewTopChannel> channelRed = channelMap.reduceByKey(new NewTopChannelReduce());
			JavaPairRDD<Long, NewTopChannel> total = channelRed.mapToPair(new TopChangelTotalMap());

			log.info("start sort total --------------------------------------------------");
			JavaPairRDD<Long, NewTopChannel> totalSort = total.sortByKey(false);
			result.put("total", getResultFromSortedRdd(totalSort));
			log.info("end sort total --------------------------------------------------");
			
			log.info("start sort neg --------------------------------------------------");
			JavaPairRDD<Long, NewTopChannel> neg = channelRed.mapToPair(new TopChangelNegMap());

			JavaPairRDD<Long, NewTopChannel> negSort = neg.sortByKey(false);
			result.put("neg", getResultFromSortedRdd(negSort));
			log.info("end sort neg --------------------------------------------------");
			

			log.info("start sort neu --------------------------------------------------");
			JavaPairRDD<Long, NewTopChannel> neu = channelRed.mapToPair(new TopChangelNeuMap());

			JavaPairRDD<Long, NewTopChannel> neuSort = neu.sortByKey(false);
			result.put("neu", getResultFromSortedRdd(neuSort));
			log.info("end sort neu --------------------------------------------------");

			log.info("start sort pos --------------------------------------------------");
			JavaPairRDD<Long, NewTopChannel> pos = channelRed.mapToPair(new TopChangelPosMap());

			JavaPairRDD<Long, NewTopChannel> posSort = pos.sortByKey(false);
			result.put("pos", getResultFromSortedRdd(posSort));
			log.info("end sort pos --------------------------------------------------");
			
			
			log.info("start sort total comment --------------------------------------------------");
			JavaPairRDD<Long, NewTopChannel> totalComment = channelRed.mapToPair(new TopChangelCommentTotalMap());
			JavaPairRDD<Long, NewTopChannel> totalCommentSort = totalComment.sortByKey(false);
			result.put("totalComment", getResultFromSortedRdd(totalCommentSort));
			log.info("end sort total comment --------------------------------------------------");
			
			log.info("start sort neg comment--------------------------------------------------");
			JavaPairRDD<Long, NewTopChannel> negComment = channelRed.mapToPair(new TopChangelCommentNegMap());

			JavaPairRDD<Long, NewTopChannel> negCommentSort = negComment.sortByKey(false);
			result.put("negComment", getResultFromSortedRdd(negCommentSort));
			log.info("end sort neg comment--------------------------------------------------");
			

			log.info("start sort neu comment--------------------------------------------------");
			JavaPairRDD<Long, NewTopChannel> neuCommnet = channelRed.mapToPair(new TopChangelCommentNeuMap());

			JavaPairRDD<Long, NewTopChannel> neuCommentSort = neuCommnet.sortByKey(false);
			result.put("neuComment", getResultFromSortedRdd(neuCommentSort));
			log.info("end sort neu commnet --------------------------------------------------");

			log.info("start sort pos comment--------------------------------------------------");
			JavaPairRDD<Long, NewTopChannel> posComment = channelRed.mapToPair(new TopChangelCommentPosMap());

			JavaPairRDD<Long, NewTopChannel> posCommentSort = posComment.sortByKey(false);
			result.put("posCommnet", getResultFromSortedRdd(posCommentSort));
			log.info("end sort pos comment--------------------------------------------------");

			snsDocs.unpersist();
		} else {
			List<NewTopChannel> emptyList = new ArrayList<>();
			Map<String, List<NewTopChannel>> topChannelRes = new HashMap<String, List<NewTopChannel>>();
			topChannelRes.put("all", emptyList);
			topChannelRes.put("facebook", emptyList);
			topChannelRes.put("twitter", emptyList);
			topChannelRes.put("weibo", emptyList);
			topChannelRes.put("youtube", emptyList);
			topChannelRes.put("instagram", emptyList);
			topChannelRes.put("weixin", emptyList);
			topChannelRes.put("plurk", emptyList);
			topChannelRes.put("lineq", emptyList);
			result.put("total", topChannelRes);
			result.put("neg", topChannelRes);
			result.put("neu", topChannelRes);
			result.put("pos", topChannelRes);
		}
		return result;
	}

	private Map<String, List<TopWebsiteEntity>> topWebsiteStatistic(Date sDate, Date eDate, boolean isTargeted) throws Exception {
		Map<String, List<TopWebsiteEntity>> result = new HashMap<String, List<TopWebsiteEntity>>();
		JavaRDD<SolrDocument> allDocs = loadOthersDocRdd(sDate, eDate, isTargeted);
		System.out.println("top website all docs number:"+ allDocs.count());
		System.out.println("start top website statistic xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		if (allDocs != null) {
			allDocs.persist(StorageLevel.MEMORY_AND_DISK());
			JavaPairRDD<String, TopWebsiteEntity> websiteMap = allDocs.mapToPair(new TopWebsiteMap());
			JavaPairRDD<String, TopWebsiteEntity> websiteRed = websiteMap.reduceByKey(new TopWebsiteReduce());

			System.out.println("start websit sort total --------------------------------------------------");
			JavaPairRDD<Long, TopWebsiteEntity> total = websiteRed.mapToPair(new TopWebsiteTotalMap());
			result.put("total", getTopWebsiteListFromTupleList(total.sortByKey(false).take(50)));

			System.out.println("start websit sort neg --------------------------------------------------");
			JavaPairRDD<Long, TopWebsiteEntity> neg = websiteRed.mapToPair(new TopWebsiteNegMap());
			result.put("neg", getTopWebsiteListFromTupleList(neg.sortByKey(false).take(50)));

			System.out.println("start websit sort neu --------------------------------------------------");
			JavaPairRDD<Long, TopWebsiteEntity> neu = websiteRed.mapToPair(new TopWebsiteNeuMap());
			result.put("neu", getTopWebsiteListFromTupleList(neu.sortByKey(false).take(50)));

			System.out.println("start websit sort pos --------------------------------------------------");
			JavaPairRDD<Long, TopWebsiteEntity> pos = websiteRed.mapToPair(new TopWebsitePosMap());
			result.put("pos", getTopWebsiteListFromTupleList(pos.sortByKey(false).take(50)));
			
			System.out.println("start websit sort total comment --------------------------------------------------");
			JavaPairRDD<Long, TopWebsiteEntity> totalComment = websiteRed.mapToPair(new TopWebsiteCommentTotalMap());
			result.put("totalComment", getTopWebsiteListFromTupleList(totalComment.sortByKey(false).take(50)));

			System.out.println("start websit sort neg comment --------------------------------------------------");
			JavaPairRDD<Long, TopWebsiteEntity> negComment = websiteRed.mapToPair(new TopWebsiteCommentNegMap());
			result.put("negComment", getTopWebsiteListFromTupleList(negComment.sortByKey(false).take(50)));

			System.out.println("start websit sort neu comment --------------------------------------------------");
			JavaPairRDD<Long, TopWebsiteEntity> neuComment = websiteRed.mapToPair(new TopWebsiteCommentNeuMap());
			result.put("neuComment", getTopWebsiteListFromTupleList(neuComment.sortByKey(false).take(50)));

			System.out.println("start websit sort pos comment --------------------------------------------------");
			JavaPairRDD<Long, TopWebsiteEntity> posComment = websiteRed.mapToPair(new TopWebsiteCommentPosMap());
			result.put("posComment", getTopWebsiteListFromTupleList(posComment.sortByKey(false).take(50)));

			allDocs.unpersist();
		} else {
			List<TopWebsiteEntity> emptyList = new ArrayList<>();
			result.put("total", emptyList);
			result.put("neg", emptyList);
			result.put("neu", emptyList);
			result.put("pos", emptyList);
		}

		return result;
	}

	private List<TopWebsiteEntity> getTopWebsiteListFromTupleList(List<Tuple2<Long, TopWebsiteEntity>> list) {
		List<TopWebsiteEntity> websiteList = new ArrayList<TopWebsiteEntity>();
		for (Tuple2<Long, TopWebsiteEntity> tuple : list) {
			websiteList.add(tuple._2);
		}
		return websiteList;
	}

	private List<NewTopChannel> getTopChannelListFromTupleList(List<Tuple2<Long, NewTopChannel>> list) {
		List<NewTopChannel> channelList = new ArrayList<NewTopChannel>();
		for (Tuple2<Long, NewTopChannel> tuple : list) {
			channelList.add(tuple._2);
		}
		return channelList;
	}

	private Map<String, List<NewTopChannel>> getResultFromSortedRdd(JavaPairRDD<Long, NewTopChannel> sortRdd) {
		Map<String, List<NewTopChannel>> topChannelRes = new HashMap<String, List<NewTopChannel>>();

		List<Tuple2<Long, NewTopChannel>> totalList = sortRdd.take(50);
		topChannelRes.put("all", getTopChannelListFromTupleList(totalList));

		List<Tuple2<Long, NewTopChannel>> fbList = sortRdd.filter(new TopChangelFacebookFilter()).take(50);
		topChannelRes.put("facebook", getTopChannelListFromTupleList(fbList));

		List<Tuple2<Long, NewTopChannel>> twitterList = sortRdd.filter(new TopChangelTwitterFilter()).take(50);
		topChannelRes.put("twitter", getTopChannelListFromTupleList(twitterList));

		List<Tuple2<Long, NewTopChannel>> weiboList = sortRdd.filter(new TopChangelWeiboFilter()).take(50);
		topChannelRes.put("weibo", getTopChannelListFromTupleList(weiboList));

		List<Tuple2<Long, NewTopChannel>> youtubeList = sortRdd.filter(new TopChangelYoutubeFilter()).take(50);
		topChannelRes.put("youtube", getTopChannelListFromTupleList(youtubeList));

		List<Tuple2<Long, NewTopChannel>> instagramList = sortRdd.filter(new TopChangelInstagramFilter()).take(50);
		topChannelRes.put("instagram", getTopChannelListFromTupleList(instagramList));

		List<Tuple2<Long, NewTopChannel>> weixinList = sortRdd.filter(new TopChangelWeixinFilter()).take(50);
		topChannelRes.put("weixin", getTopChannelListFromTupleList(weixinList));

		List<Tuple2<Long, NewTopChannel>> plurkList = sortRdd.filter(new TopChangelPlurkFilter()).take(50);
		topChannelRes.put("plurk", getTopChannelListFromTupleList(plurkList));

		List<Tuple2<Long, NewTopChannel>> lineqList = sortRdd.filter(new TopChangelLineqFilter()).take(50);
		topChannelRes.put("lineq", getTopChannelListFromTupleList(lineqList));

		return topChannelRes;
	}

	private JavaRDD<SolrDocument> loadAllDocRdd(Date sDate, Date eDate, boolean isTargeted) throws SolrServerException {
		Set<String> excluded = profile.getExcludeKeywords();
		String queryStr = SolrQueryUtil.getLanguages(profile.getLanguages()).toString();
		StringBuilder sb = new StringBuilder(queryStr);
		if (isTargeted) {
			Set<String> whiteList = profile.getWhiteList();
			Set<String> blackList = profile.getBlackList();
			if (whiteList.isEmpty() && blackList.isEmpty()) {
				return null;
			} else {
				sb.append(SolrQueryUtil.getWhiteBlackList(profile.getWhiteList(), profile.getBlackList()));
			}
		}
		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), SolrCollection.getAllCollectionString());
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), sb.toString(), sDate, eDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.SNS_TYPE, SolrFieldDefinition.DOMAIN,
						SolrFieldDefinition.AUTHOR, SolrFieldDefinition.USER_NAME, SolrFieldDefinition.SENTIMENT_SCORE,
						SolrFieldDefinition.LIKE_COUNT, SolrFieldDefinition.SHARE_COUNT, SolrFieldDefinition.COMMENT_COUNT,
						SolrFieldDefinition.READ_COUNT, SolrFieldDefinition.DISLIKE_COUNT, SolrFieldDefinition.SNS_TYPE,
						SolrFieldDefinition.PUBLISH_DATE, SolrFieldDefinition.FB_ANGRY, SolrFieldDefinition.FB_HAHA,
						SolrFieldDefinition.FB_LOVE, SolrFieldDefinition.FB_SAD, SolrFieldDefinition.FB_THANKFUL,
						SolrFieldDefinition.FB_WOW }, SolrCollection.getAllCollectionString());
		System.out.println("Query: " + solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	private JavaRDD<SolrDocument> loadOthersDocRdd(Date sDate, Date eDate, boolean isTargeted) throws SolrServerException {
		Set<String> excluded = profile.getExcludeKeywords();
		String queryStr = SolrQueryUtil.getLanguages(profile.getLanguages()).toString();
		StringBuilder sb = new StringBuilder(queryStr);
		if (isTargeted) {
			Set<String> whiteList = profile.getWhiteList();
			Set<String> blackList = profile.getBlackList();
			if (whiteList.isEmpty() && blackList.isEmpty()) {
				return null;
			} else {
				sb.append(SolrQueryUtil.getWhiteBlackList(profile.getWhiteList(), profile.getBlackList()));
			}
		}
		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), SolrCollection.OTHERS.getValue());
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), sb.toString(), sDate, eDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.SNS_TYPE, SolrFieldDefinition.DOMAIN,
						SolrFieldDefinition.AUTHOR, SolrFieldDefinition.USER_NAME, SolrFieldDefinition.SENTIMENT_SCORE,
						SolrFieldDefinition.LIKE_COUNT, SolrFieldDefinition.SHARE_COUNT, SolrFieldDefinition.COMMENT_COUNT,
						SolrFieldDefinition.READ_COUNT, SolrFieldDefinition.DISLIKE_COUNT, SolrFieldDefinition.SNS_TYPE,
						SolrFieldDefinition.PUBLISH_DATE, SolrFieldDefinition.FB_ANGRY, SolrFieldDefinition.FB_HAHA,
						SolrFieldDefinition.FB_LOVE, SolrFieldDefinition.FB_SAD, SolrFieldDefinition.FB_THANKFUL,
						SolrFieldDefinition.FB_WOW }, SolrCollection.OTHERS.getValue());
		System.out.println("Query: " + solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	private JavaRDD<SolrDocument> loadSNSDocRdd(Date sDate, Date eDate, boolean isTargeted) throws SolrServerException {
		Set<String> excluded = profile.getExcludeKeywords();
		String queryStr = SolrQueryUtil.getLanguages(profile.getLanguages()).toString();
		StringBuilder sb = new StringBuilder(queryStr);
		if (isTargeted) {
			Set<String> whiteList = profile.getWhiteList();
			Set<String> blackList = profile.getBlackList();
			if (whiteList.isEmpty() && blackList.isEmpty()) {
				return null;
			} else {
				sb.append(SolrQueryUtil.getWhiteBlackList(profile.getWhiteList(), profile.getBlackList()));
			}
		}
		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), "social-media");
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), sb.toString(), sDate, eDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.SNS_TYPE, SolrFieldDefinition.SENTIMENT_SCORE,
						SolrFieldDefinition.POS_THEMES, SolrFieldDefinition.NEG_THEMES, SolrFieldDefinition.NEUTRAL_THEMES,
						SolrFieldDefinition.LIKE_COUNT, SolrFieldDefinition.SHARE_COUNT, SolrFieldDefinition.COMMENT_COUNT,
						SolrFieldDefinition.READ_COUNT, SolrFieldDefinition.FB_ANGRY, SolrFieldDefinition.FB_HAHA,
						SolrFieldDefinition.FB_LOVE, SolrFieldDefinition.FB_SAD, SolrFieldDefinition.FB_THANKFUL,
						SolrFieldDefinition.FB_WOW, SolrFieldDefinition.USER_PAGE_LINK, SolrFieldDefinition.USER_ID,
						SolrFieldDefinition.USER_NAME }, "social-media");
		System.out.println("Query: " + solrQuery.toString());
		JavaRDD<SolrDocument> allDocs = solrRDD.queryShards(context, solrQuery);
		return allDocs;
	}

	public static Gson gsonUtil() {
		if (gson == null) {
			gson = JsonUtil.newGson();
		}
		return gson;
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
		return SparkDef.BUZZ_STATISTICS_FILTER_TASK_REQUEST_HASH;
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

	public DashboardProfile getProfile() {
		return profile;
	}

}

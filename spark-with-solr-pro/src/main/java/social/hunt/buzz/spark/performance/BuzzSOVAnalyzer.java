package social.hunt.buzz.spark.performance;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import social.hunt.buzz.spark.common.SparkProfileAnalyzer;
import social.hunt.buzz.spark.data.SOVEntity;
import social.hunt.buzz.spark.performance.data.BuzzAnalysisJobInput;
import social.hunt.buzz.spark.performance.function.SovMap;
import social.hunt.buzz.spark.performance.function.SovReduce;
import social.hunt.data.domain.DashboardProfile;
import social.hunt.data.domain.SOVCategory;
import social.hunt.mongo.connection.MongoClientManager;
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
import com.sa.redis.definition.RedisDefinition.SparkDef;

public class BuzzSOVAnalyzer extends SparkProfileAnalyzer<BuzzAnalysisJobInput> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4850381901629525845L;

	private static final Logger log = LoggerFactory.getLogger(BuzzSOVAnalyzer.class);

	private final String appName;
	private final JavaSparkContext context;
	private final DashboardProfile profile;
	private final BuzzAnalysisJobInput jobInput;

	private final String taskId;
	private static Gson gson;

	private List<String> exceptions;
	private SparkConf sparkConf;

	/**
	 * @param taskId
	 * @throws Exception
	 */
	public BuzzSOVAnalyzer(String taskId) throws Exception {
		super();
		this.taskId = taskId;
		jobInput = this.getJobInput(taskId);
		this.profile = jobInput.getProfile();

		appName = "Buzz SOV Statistic - " + profile.getId();
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

		BuzzSOVAnalyzer instance = null;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		MongoCredential credential = MongoCredential.createCredential(CommonConfig.getInstance().getMongoDBUser(), "admin", CommonConfig
				.getInstance().getMongoDBPassword().toCharArray());
		MongoClient mongoClient = new MongoClient(new ServerAddress(CommonConfig.getInstance().getMongoDBHost(), CommonConfig.getInstance()
				.getMongoDBPort()), Arrays.asList(credential));

		MongoDatabase shareOfVoice = mongoClient.getDatabase("shareOfVoice");
		UpdateOptions options = new UpdateOptions().upsert(true);

		Date s365Date = DateUtils.getNDaysBefore(365);
		s365Date = DateUtils.clearTime(s365Date);
		Date endDate = new Date();

		try {
			String taskId = args[0];
			instance = new BuzzSOVAnalyzer(taskId);

			System.out.println("start calculate -----------------------------------------------------" + instance.getProfile().getId());

			MongoCollection<Document> coll = shareOfVoice.getCollection("profile_" + instance.getProfile().getId());
			List<SOVEntity> sovRes = instance.sovCategorize(s365Date, endDate);
			for (SOVEntity entity : sovRes) {
				String jsonStr = gsonUtil().toJson(entity);
				Document doc = Document.parse(jsonStr);
				doc.append("_id", entity.getDateStr() + "_" + entity.getCatName()).append("date", df.parse(entity.getDateStr()));
				coll.updateOne(new Document("_id", entity.getDateStr() + "_" + entity.getCatName()), new Document("$set", doc), options);
			}

			instance.submitResult("finish sov statistic for profile:" + instance.getProfile().getId());

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			System.out.println(e.getMessage());
			instance.submitResult("Fail sov statistic for profile:" + instance.getProfile().getId() + "    Exception:" + e.getMessage());
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

	private List<SOVEntity> sovCategorize(Date sDate, Date eDate) throws SolrServerException {
		Set<SOVCategory> sovCategories = profile.getSovCategories();
		List<SOVEntity> sovList = new ArrayList<SOVEntity>();

		for (SOVCategory category : sovCategories) {
			String sovQueryStr = SolrQueryUtil.toKeywordQuery(category.getAndKeywordCollection(), category.getOrKeywordCollection())
					.toString();
			JavaRDD<SolrDocument> allDocs = loadAllDocRdd(sDate, eDate, sovQueryStr);

			System.out.println("start allDocs -----------------------------------------------------" + allDocs.count());

			JavaPairRDD<String, SOVEntity> engagementMap = allDocs.mapToPair(new SovMap());
			JavaPairRDD<String, SOVEntity> sovReduce = engagementMap.reduceByKey(new SovReduce());
			Map<String, SOVEntity> catResult = sovReduce.collectAsMap();
			for (SOVEntity entity : catResult.values()) {
				entity.setCatName(category.getName());
				sovList.add(entity);
			}
		}
		return sovList;
	}

	private JavaRDD<SolrDocument> loadAllDocRdd(Date startDate, Date endDate, String sovQuery) throws SolrServerException {
		Set<String> excluded = profile.getExcludeKeywords();
		StringBuilder sb = new StringBuilder(sovQuery);
		sb.append(SolrQueryUtil.getLanguages(profile.getLanguages()));

		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), SolrCollection.getAllCollectionString());
		SolrQuery solrQuery = SolrQueryUtil.getSolrQueryForSpark(profile.getAndKeywordsCollection(), profile.getKeywords(),
				profile.getSourceTypes(), profile.getSnses(), profile.getRegions(), sb.toString(), startDate, endDate, excluded,
				new SolrFieldDefinition[] { SolrFieldDefinition.URL, SolrFieldDefinition.LIKE_COUNT, SolrFieldDefinition.SHARE_COUNT,
						SolrFieldDefinition.COMMENT_COUNT, SolrFieldDefinition.READ_COUNT, SolrFieldDefinition.DISLIKE_COUNT,
						SolrFieldDefinition.PUBLISH_DATE, SolrFieldDefinition.FB_ANGRY, SolrFieldDefinition.FB_HAHA,
						SolrFieldDefinition.FB_LOVE, SolrFieldDefinition.FB_SAD, SolrFieldDefinition.FB_THANKFUL,
						SolrFieldDefinition.FB_WOW }, SolrCollection.getAllCollectionString());
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
		return SparkDef.BUZZ_STATISTICS_SOV_TASK_REQUEST_HASH;
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

/**
 * 
 */
package social.hunt.buzz.spark.common;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;

import scala.collection.mutable.WrappedArray;
import social.hunt.buzz.spark.data.MediaDistributionEntity;
import social.hunt.buzz.spark.data.NewTopChannel;
import social.hunt.buzz.spark.data.SOVEntity;
import social.hunt.buzz.spark.data.TopChannelEntity;
import social.hunt.buzz.spark.data.TopWebsiteEntity;
import social.hunt.buzz.spark.data.TrendEntity;
import social.hunt.buzz.spark.performance.BuzzAnalyzer;
import social.hunt.buzz.spark.performance.data.PerformanceResult;
import social.hunt.buzz.spark.performance.function.CountingLong;
import social.hunt.buzz.spark.performance.function.CoveragePairFunction;
import social.hunt.buzz.spark.performance.function.UserCount;
import social.hunt.buzz.spark.sentiment.SentimentAnalyzer;
import social.hunt.buzz.spark.sentiment.comparator.EntityTupleComparator;
import social.hunt.buzz.spark.sentiment.comparator.EntityTuplePairComparator;
import social.hunt.buzz.spark.sentiment.function.EntityTypeFilterFunction;
import social.hunt.buzz.spark.sentiment.function.NameEntityCount;
import social.hunt.buzz.spark.sentiment.function.SentimentalEntityCount;
import social.hunt.data.domain.BuzzAnalysisData;
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

import com.google.gson.Gson;
import com.lucidworks.spark.SolrRDD;
import com.sa.common.config.CommonConfig;
import com.sa.common.json.JsonUtil;
import com.sa.redis.util.RedisUtil;

/**
 * @author lewis
 *
 */
public abstract class SparkProfileAnalyzer<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9072200218308190980L;
	private Gson gson;

	/**
	 * 
	 */
	public SparkProfileAnalyzer() {
	}

	protected abstract String getJobInputHash();

	protected abstract Class<T> getJobInputType();

	protected abstract String getTaskId();

	protected abstract String getAppName();

	protected abstract Logger getLog();

	protected Gson getGson() {
		if (gson == null) {
			gson = JsonUtil.newGson();
		}
		return gson;
	}

	protected T getJobInput(String taskId) throws Exception {
		try {
			String val = RedisUtil.getInstance().hget(getJobInputHash(), taskId);
			if (StringUtils.isBlank(val)) {
				// TODO
			} else {
				return getGson().fromJson(val, getJobInputType());
			}
		} catch (Exception e) {
			throw e;
		}
		return null;
	}

	protected void submitResult(Object dataObject) throws Exception {
		RedisUtil.getInstance().publish(getTaskId(), getGson().toJson(dataObject));
	}

	protected void testTopChanel(Object dataObject) throws Exception {
		RedisUtil.getInstance().rpush("test-topChannel", getGson().toJson(dataObject));
	}

	/**
	 * <a href= "http://spark.apache.org/docs/latest/configuration.html">http://spark.apache.org/docs/latest/configuration.html</a>
	 * 
	 * @return
	 */
	protected SparkConf newSparkConf() throws Exception {
		getLog().info("newSparkConf");

		SparkConf sparkConf = new SparkConf()
				.setMaster(CommonConfig.getInstance().getSparkMaster())
				/**
				 * The name of your application. This will appear in the UI and in log data.
				 */
				.setAppName(getAppName())
				/**
				 * Number of cores to use for the driver process, only in cluster mode.
				 */
				.set("spark.driver.cores", "1")
				/**
				 * Amount of memory to use per executor process, in the same format as JVM memory strings (e.g. 512m, 2g).
				 */
				.set("spark.executor.memory", CommonConfig.getInstance().getSparkMemory())
				.set("spark.cores.max", "20")
				/**
				 * Logs the effective SparkConf as INFO when a SparkContext is started.
				 */
				.set("spark.logConf", "true")
				/**
				 * Enable Kyro serialization
				 */
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				/**
				 * Register Kryo serialization object
				 */
				.registerKryoClasses(
						new Class[] { JavaSparkContext.class, SolrRDD.class, SolrDocument.class, BuzzAnalyzer.class,
								SentimentAnalyzer.class, DashboardProfile.class, WrappedArray.class, WrappedArray.ofRef.class,
								UserCount.class, CoveragePairFunction.class, CountingLong.class, BuzzAnalysisData.class,
								PerformanceResult.class, SentimentAnalysisData.class, EntityTupleComparator.class,
								SentimentalEntityCount.class, NameEntityCount.class, SentimentAnalysisPattern.class,
								SentimentAnalysisProduct.class, SentimentAnalysisCompany.class, SentimentAnalysisJobTitle.class,
								SentimentAnalysisKeyword.class, SentimentAnalysisTheme.class, SentimentAnalysisPerson.class,
								SentimentAnalysisPlace.class, EntityTypeFilterFunction.class, EntityTuplePairComparator.class,
								NewTopChannel.class, TopWebsiteEntity.class, MediaDistributionEntity.class, TrendEntity.class,
								TopChannelEntity.class, SOVEntity.class })
				/**
				 * Kyro registration not required
				 */
				.set("spark.kryo.registrationRequired", "false")
				/**
				 * Limit of total size of serialized results of all partitions for each Spark action (e.g. collect). Should be at least 1M,
				 * or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause
				 * out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper
				 * limit can protect the driver from out-of-memory errors.
				 */
				.set("spark.driver.maxResultSize", CommonConfig.getInstance().getSparkMemory()).setJars(JavaSparkContext.jarOfObject(this));

		return sparkConf;
	}
}

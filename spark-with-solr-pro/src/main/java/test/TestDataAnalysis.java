package test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

import scala.Tuple2;
import social.hunt.buzz.spark.common.function.SumLong;
import social.hunt.buzz.spark.performance.definition.ResultType;
import social.hunt.buzz.spark.performance.function.CountingLong;
import social.hunt.buzz.spark.performance.function.CoveragePairFunction;
import social.hunt.buzz.spark.sentiment.definition.SentimentDef;
import social.hunt.buzz.spark.sentiment.function.DocSentimentCount;
import social.hunt.solr.definition.SolrCollection;

import com.google.gson.Gson;
import com.lucidworks.spark.SolrRDD;
import com.sa.common.config.CommonConfig;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.DateUtils;

public class TestDataAnalysis {

	public static void main(String[] args) throws Exception {
		Gson gson = new Gson();

		SparkConf sparkConf = new SparkConf().setAppName("TestBuzzAnalyser");
		sparkConf.setMaster("spark://solr-node2:7077");
		String[] jars = new String[1];
		jars[0] = "target/buzz-statistics-spark-0.6.3-SNAPSHOT-jar-with-dependencies.jar";
		sparkConf.setJars(jars);
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<SolrDocument> allDocs = loadAllDocRdd(new Date(), DateUtils.getNDaysBefore(90), ctx)
				.persist(StorageLevel.MEMORY_AND_DISK());
		System.out.println("Size of solr docs: " + allDocs.count());

		JavaPairRDD<ResultType, Long> javaPairRdd = allDocs.flatMapToPair(new CountingLong());
		Map<ResultType, Long> resultLong = javaPairRdd.reduceByKeyLocally(new SumLong());
		

		System.out.println("Size of solr docs with 1: " + gson.toJson(resultLong));
		//System.out.println("Size of solr docs with 2: " + gson.toJson(s));
		

		allDocs.unpersist();
		ctx.stop();
	}

	public static JavaRDD<SolrDocument> loadAllDocRdd(Date startDate, Date endDate, JavaSparkContext context) throws SolrServerException {
		List<String> collectionList = new ArrayList<String>();

		collectionList.add(SolrCollection.OTHERS.getValue());
		collectionList.add(SolrCollection.SOCIAL_MEDIA.getValue());
		String collections = StringUtils.join(collectionList, ",");

		SolrRDD solrRDD = new SolrRDD(CommonConfig.getInstance().getNewDocumentIndexerZookeeper(), collections);
		SolrQuery solrQ = new SolrQuery();
//		solrQ.setQuery("+sourceType:(4 99)+pDate:[2016-02-21T16:00:00Z TO 2016-02-22T02:20:50Z]");
		solrQ.setQuery("*:*");
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
}

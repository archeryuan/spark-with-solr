package social.hunt.buzz.spark.performance;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import social.hunt.buzz.spark.performance.data.BuzzMonitoringJobOutput;
import social.hunt.buzz.spark.performance.data.domain.EngagementPostCount;
import social.hunt.buzz.spark.performance.data.domain.MonitorKeyValueData;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.gson.Gson;
import com.sa.common.json.JsonUtil;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.domain.persistence.BuzzPost;

public final class BuzzMonitoringAnalyzer {
	private static final Pattern SPACE = Pattern.compile("\r\n");
	private static final Logger log = LoggerFactory.getLogger(BuzzMonitoringAnalyzer.class);
	private static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
	private static JavaSparkContext ctx;

	public static void main(String[] args) throws Exception {
		Gson gson = new Gson();

		if (args.length < 2) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}
		String taskId = args[0];
		String HDFSPath = args[1];
		String appName = "BuzzMonitoringAnalyzer";
		if (!StringUtils.isBlank(HDFSPath)) {
			try {
				appName = appName + "-" + HDFSPath.split("/")[3];
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}
		BuzzMonitoringJobOutput result = new BuzzMonitoringJobOutput();
		SparkConf sparkConf = new SparkConf().setAppName(appName);
		ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(HDFSPath, 1);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -8363202893203547358L;

			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		JavaPairRDD<String, EngagementPostCount> engagementMap = words.mapToPair(new PairFunction<String, String, EngagementPostCount>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 4311337082234212095L;

			@Override
			public Tuple2<String, EngagementPostCount> call(String s) throws JsonParseException, JsonMappingException, IOException {
				String[] array = s.split("#");
				EngagementPostCount mapValue = null;
				String key = array[0].split("_")[0];
				try {
					if (array.length > 1) {
						String value = s.split("#")[1];
						BuzzPost post = JsonUtil.getMapper().readValue(value, BuzzPost.class);
						key = format.format(post.getPublishDate());
						long engagement = post.getLikeCount().intValue() + post.getCommentCount().intValue()
								+ post.getShareCount().intValue();
						mapValue = new EngagementPostCount(engagement, 1l);
					}
				} catch (Exception ex) {
					log.error(ex.getMessage(), ex);
				}
				return new Tuple2<String, EngagementPostCount>(key, mapValue);
			}
		});

		JavaPairRDD<String, EngagementPostCount> engagementReduce = engagementMap
				.reduceByKey(new Function2<EngagementPostCount, EngagementPostCount, EngagementPostCount>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 5627738673621186264L;

					@Override
					public EngagementPostCount call(EngagementPostCount i1, EngagementPostCount i2) {

						long enegagement = i1.getEngagement() + i2.getEngagement();
						long postCount = i1.getPostCount() + i2.getPostCount();

						return new EngagementPostCount(enegagement, postCount);
					}
				});

		List<Tuple2<String, EngagementPostCount>> output = engagementReduce.collect();
		List<MonitorKeyValueData> dataList = new ArrayList<MonitorKeyValueData>();
		for (Tuple2<?, ?> tuple : output) {
			EngagementPostCount value = (EngagementPostCount) tuple._2;
			System.out.println(tuple._1().toString() + " " + value.getEngagement() + " " + value.getPostCount());
			dataList.add(new MonitorKeyValueData(tuple._1().toString(), value));
		}

		result.setDailyPostList(dataList);

		log.info("dataList {}", result.toString());
		// RedisUtil.getInstance().publish(taskId, JsonUtil.getMapper().writeValueAsString(result));
		RedisUtil.getInstance().publish(taskId, gson.toJson(result));
		log.info("finish publish job: {}", taskId);
		ctx.stop();
	}
}
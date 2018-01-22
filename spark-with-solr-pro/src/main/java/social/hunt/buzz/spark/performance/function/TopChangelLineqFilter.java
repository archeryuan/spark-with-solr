package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import social.hunt.buzz.spark.data.NewTopChannel;

public class TopChangelLineqFilter implements Function<Tuple2<Long, NewTopChannel>, Boolean>, Serializable {

	private static final long serialVersionUID = -863558569855018750L;

	@Override
	public Boolean call(Tuple2<Long, NewTopChannel> tuple) {
		return tuple._2.getMedia().startsWith("Lineq");
	}
}
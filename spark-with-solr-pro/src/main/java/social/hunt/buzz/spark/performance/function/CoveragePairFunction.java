/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author lewis
 *
 */
public class CoveragePairFunction extends FunctionUtils implements PairFunction<SolrDocument, String, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7545339323637362040L;

	@Override
	public Tuple2<String, Long> call(SolrDocument doc) throws Exception {
		String domain = extractDomainFromDoc(doc);
		if (StringUtils.endsWith(domain, ".tw") || StringUtils.endsWith(domain, ".cn") || "zhidao.baidu.com".equals(domain)) {
			return new Tuple2<String, Long>("", 0L);
		}
		return new Tuple2<String, Long>(domain, 1L);
	}

}

/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.Function;

/**
 * @author lewis
 *
 */
public class CountAsOne implements Function<SolrDocument, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	@Override
	public Long call(SolrDocument arg0) throws Exception {
		return 1L;
	}

}

/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.function.Function;

import com.sa.common.definition.SolrFieldDefinition;

/**
 * @author lewis
 *
 */
public class UserCount extends FunctionUtils implements Function<SolrDocument, String>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(SolrDocument doc) throws Exception {
		String domain = extractDomainFromDoc(doc);
		if (!StringUtils.isBlank(domain) && doc.containsKey(SolrFieldDefinition.USER_ID.getName())) {
			return StringUtils.join(new String[] { domain, (String) doc.getFieldValue(SolrFieldDefinition.USER_ID.getName()) }, ":");
		}
		return "";
	}
}

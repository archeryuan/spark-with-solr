/**
 * 
 */
package social.hunt.buzz.spark.performance.function;

import java.io.Serializable;

import org.apache.solr.common.SolrDocument;

import social.hunt.common.definition.Sns;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.StringUtils;
import com.sa.common.util.UrlUtil;

/**
 * @author lewis
 *
 */
public class FunctionUtils implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public FunctionUtils() {
		super();
	}

	public static String extractDomainFromDoc(SolrDocument doc) {
		if (doc.containsKey(SolrFieldDefinition.DOMAIN.getName())) {
			String tmp = (String) doc.getFieldValue(SolrFieldDefinition.DOMAIN.getName());
			if (!StringUtils.isBlank(tmp))
				return tmp;
		}

		if (doc.containsKey(SolrFieldDefinition.SNS_TYPE.getName())) {
			Sns sns = Sns.getSns((int) doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName()));
			String domain = null;
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
			// case WEIXIN:
			// domain = "qq.com";
			// break;
			case YOUTUBE:
				domain = "youtube.com";
				break;
			}

			if (!StringUtils.isBlank(domain))
				return domain;
		}

		String domain = StringUtils.defaultString(UrlUtil.extractDomain((String) doc.getFieldValue(SolrFieldDefinition.URL.getName())));
		return domain;
	}
}

/**
 * 
 */
package social.hunt.buzz.spark.sentiment.definition;

/**
 * @author lewis
 *
 */
public enum SentimentDef {
	POSITIVE(1), NEGATIVE(-1), NEUTRAL(0);

	private final short sentimentValue;

	/**
	 * @param sentimentValue
	 */
	private SentimentDef(int sentimentValue) {
		this.sentimentValue = (short) sentimentValue;
	}

	/**
	 * @return the sentimentValue
	 */
	public short getSentimentValue() {
		return sentimentValue;
	}

}

package social.hunt.buzz.spark.data;

import java.util.HashSet;
import java.util.Set;

public class TrendEntity {
	private String dateStr;
	private long webNegPost;
	private long webNeuPost;
	private long webPosPost;
	private long webTotalPost;
	private long webTotalComments;
	private long webPosComments;
	private long webNeuComments;
	private long webNegComments;

	private long facebookNegPost;
	private long facebookNeuPost;
	private long facebookPosPost;
	private long facebookTotalPost;
	private long facebookTotalComments;
	private long facebookPosComments;
	private long facebookNeuComments;
	private long facebookNegComments;

	private long twitterNegPost;
	private long twitterNeuPost;
	private long twitterPosPost;
	private long twitterTotalPost;
	private long twitterTotalComments;
	private long twitterPosComments;
	private long twitterNeuComments;
	private long twitterNegComments;

	private long weiboNegPost;
	private long weiboNeuPost;
	private long weiboPosPost;
	private long weiboTotalPost;
	private long weiboTotalComments;
	private long weiboPosComments;
	private long weiboNeuComments;
	private long weiboNegComments;

	private long youtubeNegPost;
	private long youtubeNeuPost;
	private long youtubePosPost;
	private long youtubeTotalPost;
	private long youtubeTotalComments;
	private long youtubePosComments;
	private long youtubeNeuComments;
	private long youtubeNegComments;


	private long instagramNegPost;
	private long instagramNeuPost;
	private long instagramPosPost;
	private long instagramTotalPost;
	private long instagramTotalComments;
	private long instagramPosComments;
	private long instagramNeuComments;
	private long instagramNegComments;

	private long weixinNegPost;
	private long weixinNeuPost;
	private long weixinPosPost;
	private long weixinTotalPost;
	private long weixinTotalComments;
	private long weixinPosComments;
	private long weixinNeuComments;
	private long weixinNegComments;

	private long plurkNegPost;
	private long plurkNeuPost;
	private long plurkPosPost;
	private long plurkTotalPost;
	private long plurkTotalComments;
	private long plurkPosComments;
	private long plurkNeuComments;
	private long plurkNegComments;

	private long lineqNegPost;
	private long lineqNeuPost;
	private long lineqPosPost;
	private long lineqTotalPost;
	private long lineqTotalComments;
	private long lineqPosComments;
	private long lineqNeuComments;
	private long lineqNegComments;

	private long webNegEngage;
	private long webNeuEngage;
	private long webPosEngage;
	private long webTotalEngage;

	private long facebookNegEngage;
	private long facebookNeuEngage;
	private long facebookPosEngage;
	private long facebookTotalEngage;

	private long twitterNegEngage;
	private long twitterNeuEngage;
	private long twitterPosEngage;
	private long twitterTotalEngage;

	private long weiboNegEngage;
	private long weiboNeuEngage;
	private long weiboPosEngage;
	private long weiboTotalEngage;

	private long youtubeNegEngage;
	private long youtubeNeuEngage;
	private long youtubePosEngage;
	private long youtubeTotalEngage;

	private long instagramNegEngage;
	private long instagramNeuEngage;
	private long instagramPosEngage;
	private long instagramTotalEngage;

	private long weixinNegEngage;
	private long weixinNeuEngage;
	private long weixinPosEngage;
	private long weixinTotalEngage;

	private long plurkNegEngage;
	private long plurkNeuEngage;
	private long plurkPosEngage;
	private long plurkTotalEngage;

	private long lineqNegEngage;
	private long lineqNeuEngage;
	private long lineqPosEngage;
	private long lineqTotalEngage;

	private int domainNegNum = 0;
	private int domainPosNum = 0;
	private int domainNeuNum = 0;
	private int domainNum = 0;

	private int peopleNegNum = 0;
	private int peoplePosNum = 0;
	private int peopleNeuNum = 0;
	private int peopleNum = 0;

	private Set<String> domainNeg = new HashSet<String>();
	private Set<String> domainPos = new HashSet<String>();
	private Set<String> domainNeu = new HashSet<String>();
	private Set<String> domain = new HashSet<String>();

	private Set<String> peopleNeu = new HashSet<String>();
	private Set<String> peopleNeg = new HashSet<String>();
	private Set<String> peoplePos = new HashSet<String>();
	private Set<String> people = new HashSet<String>();

	public String getDateStr() {
		return dateStr;
	}

	public void setDateStr(String dateStr) {
		this.dateStr = dateStr;
	}

	public long getWebNegPost() {
		return webNegPost;
	}

	public void setWebNegPost(long webNegPost) {
		this.webNegPost = webNegPost;
	}

	public long getWebNeuPost() {
		return webNeuPost;
	}

	public void setWebNeuPost(long webNeuPost) {
		this.webNeuPost = webNeuPost;
	}

	public long getWebPosPost() {
		return webPosPost;
	}

	public void setWebPosPost(long webPosPost) {
		this.webPosPost = webPosPost;
	}

	public long getWebTotalPost() {
		return webTotalPost;
	}

	public void setWebTotalPost(long webTotalPost) {
		this.webTotalPost = webTotalPost;
	}

	public long getFacebookNegPost() {
		return facebookNegPost;
	}

	public void setFacebookNegPost(long facebookNegPost) {
		this.facebookNegPost = facebookNegPost;
	}

	public long getFacebookNeuPost() {
		return facebookNeuPost;
	}

	public void setFacebookNeuPost(long facebookNeuPost) {
		this.facebookNeuPost = facebookNeuPost;
	}

	public long getFacebookPosPost() {
		return facebookPosPost;
	}

	public void setFacebookPosPost(long facebookPosPost) {
		this.facebookPosPost = facebookPosPost;
	}

	public long getFacebookTotalPost() {
		return facebookTotalPost;
	}

	public void setFacebookTotalPost(long facebookTotalPost) {
		this.facebookTotalPost = facebookTotalPost;
	}

	public long getTwitterNegPost() {
		return twitterNegPost;
	}

	public void setTwitterNegPost(long twitterNegPost) {
		this.twitterNegPost = twitterNegPost;
	}

	public long getTwitterNeuPost() {
		return twitterNeuPost;
	}

	public void setTwitterNeuPost(long twitterNeuPost) {
		this.twitterNeuPost = twitterNeuPost;
	}

	public long getTwitterPosPost() {
		return twitterPosPost;
	}

	public void setTwitterPosPost(long twitterPosPost) {
		this.twitterPosPost = twitterPosPost;
	}

	public long getTwitterTotalPost() {
		return twitterTotalPost;
	}

	public void setTwitterTotalPost(long twitterTotalPost) {
		this.twitterTotalPost = twitterTotalPost;
	}

	public long getWeiboNegPost() {
		return weiboNegPost;
	}

	public void setWeiboNegPost(long weiboNegPost) {
		this.weiboNegPost = weiboNegPost;
	}

	public long getWeiboNeuPost() {
		return weiboNeuPost;
	}

	public void setWeiboNeuPost(long weiboNeuPost) {
		this.weiboNeuPost = weiboNeuPost;
	}

	public long getWeiboPosPost() {
		return weiboPosPost;
	}

	public void setWeiboPosPost(long weiboPosPost) {
		this.weiboPosPost = weiboPosPost;
	}

	public long getWeiboTotalPost() {
		return weiboTotalPost;
	}

	public void setWeiboTotalPost(long weiboTotalPost) {
		this.weiboTotalPost = weiboTotalPost;
	}

	public long getYoutubeNegPost() {
		return youtubeNegPost;
	}

	public void setYoutubeNegPost(long youtubeNegPost) {
		this.youtubeNegPost = youtubeNegPost;
	}

	public long getYoutubeNeuPost() {
		return youtubeNeuPost;
	}

	public void setYoutubeNeuPost(long youtubeNeuPost) {
		this.youtubeNeuPost = youtubeNeuPost;
	}

	public long getYoutubePosPost() {
		return youtubePosPost;
	}

	public void setYoutubePosPost(long youtubePosPost) {
		this.youtubePosPost = youtubePosPost;
	}

	public long getYoutubeTotalPost() {
		return youtubeTotalPost;
	}

	public void setYoutubeTotalPost(long youtubeTotalPost) {
		this.youtubeTotalPost = youtubeTotalPost;
	}

	public long getInstagramNegPost() {
		return instagramNegPost;
	}

	public void setInstagramNegPost(long instagramNegPost) {
		this.instagramNegPost = instagramNegPost;
	}

	public long getInstagramNeuPost() {
		return instagramNeuPost;
	}

	public void setInstagramNeuPost(long instagramNeuPost) {
		this.instagramNeuPost = instagramNeuPost;
	}

	public long getInstagramPosPost() {
		return instagramPosPost;
	}

	public void setInstagramPosPost(long instagramPosPost) {
		this.instagramPosPost = instagramPosPost;
	}

	public long getInstagramTotalPost() {
		return instagramTotalPost;
	}

	public void setInstagramTotalPost(long instagramTotalPost) {
		this.instagramTotalPost = instagramTotalPost;
	}

	public long getWeixinNegPost() {
		return weixinNegPost;
	}

	public long getWeixinNeuPost() {
		return weixinNeuPost;
	}

	public long getWeixinPosPost() {
		return weixinPosPost;
	}

	public long getWeixinTotalPost() {
		return weixinTotalPost;
	}

	public void setWeixinNegPost(long weixinNegPost) {
		this.weixinNegPost = weixinNegPost;
	}

	public void setWeixinNeuPost(long weixinNeuPost) {
		this.weixinNeuPost = weixinNeuPost;
	}

	public void setWeixinPosPost(long weixinPosPost) {
		this.weixinPosPost = weixinPosPost;
	}

	public void setWeixinTotalPost(long weixinTotalPost) {
		this.weixinTotalPost = weixinTotalPost;
	}

	public long getWebNegEngage() {
		return webNegEngage;
	}

	public void setWebNegEngage(long webNegEngage) {
		this.webNegEngage = webNegEngage;
	}

	public long getWebNeuEngage() {
		return webNeuEngage;
	}

	public void setWebNeuEngage(long webNeuEngage) {
		this.webNeuEngage = webNeuEngage;
	}

	public long getWebPosEngage() {
		return webPosEngage;
	}

	public void setWebPosEngage(long webPosEngage) {
		this.webPosEngage = webPosEngage;
	}

	public long getWebTotalEngage() {
		return webTotalEngage;
	}

	public void setWebTotalEngage(long webTotalEngage) {
		this.webTotalEngage = webTotalEngage;
	}

	public long getFacebookNegEngage() {
		return facebookNegEngage;
	}

	public void setFacebookNegEngage(long facebookNegEngage) {
		this.facebookNegEngage = facebookNegEngage;
	}

	public long getFacebookNeuEngage() {
		return facebookNeuEngage;
	}

	public void setFacebookNeuEngage(long facebookNeuEngage) {
		this.facebookNeuEngage = facebookNeuEngage;
	}

	public long getFacebookPosEngage() {
		return facebookPosEngage;
	}

	public void setFacebookPosEngage(long facebookPosEngage) {
		this.facebookPosEngage = facebookPosEngage;
	}

	public long getFacebookTotalEngage() {
		return facebookTotalEngage;
	}

	public void setFacebookTotalEngage(long facebookTotalEngage) {
		this.facebookTotalEngage = facebookTotalEngage;
	}

	public long getTwitterNegEngage() {
		return twitterNegEngage;
	}

	public void setTwitterNegEngage(long twitterNegEngage) {
		this.twitterNegEngage = twitterNegEngage;
	}

	public long getTwitterNeuEngage() {
		return twitterNeuEngage;
	}

	public void setTwitterNeuEngage(long twitterNeuEngage) {
		this.twitterNeuEngage = twitterNeuEngage;
	}

	public long getTwitterPosEngage() {
		return twitterPosEngage;
	}

	public void setTwitterPosEngage(long twitterPosEngage) {
		this.twitterPosEngage = twitterPosEngage;
	}

	public long getTwitterTotalEngage() {
		return twitterTotalEngage;
	}

	public void setTwitterTotalEngage(long twitterTotalEngage) {
		this.twitterTotalEngage = twitterTotalEngage;
	}

	public long getWeiboNegEngage() {
		return weiboNegEngage;
	}

	public void setWeiboNegEngage(long weiboNegEngage) {
		this.weiboNegEngage = weiboNegEngage;
	}

	public long getWeiboNeuEngage() {
		return weiboNeuEngage;
	}

	public void setWeiboNeuEngage(long weiboNeuEngage) {
		this.weiboNeuEngage = weiboNeuEngage;
	}

	public long getWeiboPosEngage() {
		return weiboPosEngage;
	}

	public void setWeiboPosEngage(long weiboPosEngage) {
		this.weiboPosEngage = weiboPosEngage;
	}

	public long getWeiboTotalEngage() {
		return weiboTotalEngage;
	}

	public void setWeiboTotalEngage(long weiboTotalEngage) {
		this.weiboTotalEngage = weiboTotalEngage;
	}

	public long getYoutubeNegEngage() {
		return youtubeNegEngage;
	}

	public void setYoutubeNegEngage(long youtubeNegEngage) {
		this.youtubeNegEngage = youtubeNegEngage;
	}

	public long getYoutubeNeuEngage() {
		return youtubeNeuEngage;
	}

	public void setYoutubeNeuEngage(long youtubeNeuEngage) {
		this.youtubeNeuEngage = youtubeNeuEngage;
	}

	public long getYoutubePosEngage() {
		return youtubePosEngage;
	}

	public void setYoutubePosEngage(long youtubePosEngage) {
		this.youtubePosEngage = youtubePosEngage;
	}

	public long getYoutubeTotalEngage() {
		return youtubeTotalEngage;
	}

	public void setYoutubeTotalEngage(long youtubeTotalEngage) {
		this.youtubeTotalEngage = youtubeTotalEngage;
	}

	public long getInstagramNegEngage() {
		return instagramNegEngage;
	}

	public void setInstagramNegEngage(long instagramNegEngage) {
		this.instagramNegEngage = instagramNegEngage;
	}

	public long getInstagramNeuEngage() {
		return instagramNeuEngage;
	}

	public void setInstagramNeuEngage(long instagramNeuEngage) {
		this.instagramNeuEngage = instagramNeuEngage;
	}

	public long getInstagramPosEngage() {
		return instagramPosEngage;
	}

	public void setInstagramPosEngage(long instagramPosEngage) {
		this.instagramPosEngage = instagramPosEngage;
	}

	public long getInstagramTotalEngage() {
		return instagramTotalEngage;
	}

	public void setInstagramTotalEngage(long instagramTotalEngage) {
		this.instagramTotalEngage = instagramTotalEngage;
	}

	public long getWeixinNegEngage() {
		return weixinNegEngage;
	}

	public long getWeixinNeuEngage() {
		return weixinNeuEngage;
	}

	public long getWeixinPosEngage() {
		return weixinPosEngage;
	}

	public long getWeixinTotalEngage() {
		return weixinTotalEngage;
	}

	public void setWeixinNegEngage(long weixinNegEngage) {
		this.weixinNegEngage = weixinNegEngage;
	}

	public void setWeixinNeuEngage(long weixinNeuEngage) {
		this.weixinNeuEngage = weixinNeuEngage;
	}

	public void setWeixinPosEngage(long weixinPosEngage) {
		this.weixinPosEngage = weixinPosEngage;
	}

	public void setWeixinTotalEngage(long weixinTotalEngage) {
		this.weixinTotalEngage = weixinTotalEngage;
	}

	public Set<String> getDomainNeg() {
		return domainNeg;
	}

	public void setDomainNeg(Set<String> domainNeg) {
		this.domainNeg = domainNeg;
	}

	public Set<String> getDomainPos() {
		return domainPos;
	}

	public void setDomainPos(Set<String> domainPos) {
		this.domainPos = domainPos;
	}

	public Set<String> getDomainNeu() {
		return domainNeu;
	}

	public void setDomainNeu(Set<String> domainNeu) {
		this.domainNeu = domainNeu;
	}

	public Set<String> getPeopleNeu() {
		return peopleNeu;
	}

	public void setPeopleNeu(Set<String> peopleNeu) {
		this.peopleNeu = peopleNeu;
	}

	public Set<String> getPeopleNeg() {
		return peopleNeg;
	}

	public void setPeopleNeg(Set<String> peopleNeg) {
		this.peopleNeg = peopleNeg;
	}

	public Set<String> getPeoplePos() {
		return peoplePos;
	}

	public void setPeoplePos(Set<String> peoplePos) {
		this.peoplePos = peoplePos;
	}

	public Set<String> getDomain() {
		return domain;
	}

	public void setDomain(Set<String> domain) {
		this.domain = domain;
	}

	public Set<String> getPeople() {
		return people;
	}

	public void setPeople(Set<String> people) {
		this.people = people;
	}

	public int getDomainNegNum() {
		return domainNegNum;
	}

	public void setDomainNegNum(int domainNegNum) {
		this.domainNegNum = domainNegNum;
	}

	public int getDomainPosNum() {
		return domainPosNum;
	}

	public void setDomainPosNum(int domainPosNum) {
		this.domainPosNum = domainPosNum;
	}

	public int getDomainNeuNum() {
		return domainNeuNum;
	}

	public void setDomainNeuNum(int domainNeuNum) {
		this.domainNeuNum = domainNeuNum;
	}

	public int getDomainNum() {
		return domainNum;
	}

	public void setDomainNum(int domainNum) {
		this.domainNum = domainNum;
	}

	public int getPeopleNegNum() {
		return peopleNegNum;
	}

	public void setPeopleNegNum(int peopleNegNum) {
		this.peopleNegNum = peopleNegNum;
	}

	public int getPeoplePosNum() {
		return peoplePosNum;
	}

	public void setPeoplePosNum(int peoplePosNum) {
		this.peoplePosNum = peoplePosNum;
	}

	public int getPeopleNeuNum() {
		return peopleNeuNum;
	}

	public void setPeopleNeuNum(int peopleNeuNum) {
		this.peopleNeuNum = peopleNeuNum;
	}

	public int getPeopleNum() {
		return peopleNum;
	}

	public void setPeopleNum(int peopleNum) {
		this.peopleNum = peopleNum;
	}

	public long getPlurkNegPost() {
		return plurkNegPost;
	}

	public long getPlurkNeuPost() {
		return plurkNeuPost;
	}

	public long getPlurkPosPost() {
		return plurkPosPost;
	}

	public long getPlurkTotalPost() {
		return plurkTotalPost;
	}

	public long getLineqNegPost() {
		return lineqNegPost;
	}

	public long getLineqNeuPost() {
		return lineqNeuPost;
	}

	public long getLineqPosPost() {
		return lineqPosPost;
	}

	public long getLineqTotalPost() {
		return lineqTotalPost;
	}

	public long getPlurkNegEngage() {
		return plurkNegEngage;
	}

	public long getPlurkNeuEngage() {
		return plurkNeuEngage;
	}

	public long getPlurkPosEngage() {
		return plurkPosEngage;
	}

	public long getPlurkTotalEngage() {
		return plurkTotalEngage;
	}

	public long getLineqNegEngage() {
		return lineqNegEngage;
	}

	public long getLineqNeuEngage() {
		return lineqNeuEngage;
	}

	public long getLineqPosEngage() {
		return lineqPosEngage;
	}

	public long getLineqTotalEngage() {
		return lineqTotalEngage;
	}

	public void setPlurkNegPost(long plurkNegPost) {
		this.plurkNegPost = plurkNegPost;
	}

	public void setPlurkNeuPost(long plurkNeuPost) {
		this.plurkNeuPost = plurkNeuPost;
	}

	public void setPlurkPosPost(long plurkPosPost) {
		this.plurkPosPost = plurkPosPost;
	}

	public void setPlurkTotalPost(long plurkTotalPost) {
		this.plurkTotalPost = plurkTotalPost;
	}

	public void setLineqNegPost(long lineqNegPost) {
		this.lineqNegPost = lineqNegPost;
	}

	public void setLineqNeuPost(long lineqNeuPost) {
		this.lineqNeuPost = lineqNeuPost;
	}

	public void setLineqPosPost(long lineqPosPost) {
		this.lineqPosPost = lineqPosPost;
	}

	public void setLineqTotalPost(long lineqTotalPost) {
		this.lineqTotalPost = lineqTotalPost;
	}

	public void setPlurkNegEngage(long plurkNegEngage) {
		this.plurkNegEngage = plurkNegEngage;
	}

	public void setPlurkNeuEngage(long plurkNeuEngage) {
		this.plurkNeuEngage = plurkNeuEngage;
	}

	public void setPlurkPosEngage(long plurkPosEngage) {
		this.plurkPosEngage = plurkPosEngage;
	}

	public void setPlurkTotalEngage(long plurkTotalEngage) {
		this.plurkTotalEngage = plurkTotalEngage;
	}

	public void setLineqNegEngage(long lineqNegEngage) {
		this.lineqNegEngage = lineqNegEngage;
	}

	public void setLineqNeuEngage(long lineqNeuEngage) {
		this.lineqNeuEngage = lineqNeuEngage;
	}

	public void setLineqPosEngage(long lineqPosEngage) {
		this.lineqPosEngage = lineqPosEngage;
	}

	public void setLineqTotalEngage(long lineqTotalEngage) {
		this.lineqTotalEngage = lineqTotalEngage;
	}

	public long getWebTotalComments() {
		return webTotalComments;
	}

	public void setWebTotalComments(long webTotalComments) {
		this.webTotalComments = webTotalComments;
	}

	public long getFacebookTotalComments() {
		return facebookTotalComments;
	}

	public void setFacebookTotalComments(long facebookTotalComments) {
		this.facebookTotalComments = facebookTotalComments;
	}

	public long getTwitterTotalComments() {
		return twitterTotalComments;
	}

	public void setTwitterTotalComments(long twitterTotalComments) {
		this.twitterTotalComments = twitterTotalComments;
	}

	public long getWeiboTotalComments() {
		return weiboTotalComments;
	}

	public void setWeiboTotalComments(long weiboTotalComments) {
		this.weiboTotalComments = weiboTotalComments;
	}

	public long getYoutubeTotalComments() {
		return youtubeTotalComments;
	}

	public void setYoutubeTotalComments(long youtubeTotalComments) {
		this.youtubeTotalComments = youtubeTotalComments;
	}

	public long getInstagramTotalComments() {
		return instagramTotalComments;
	}

	public void setInstagramTotalComments(long instagramTotalComments) {
		this.instagramTotalComments = instagramTotalComments;
	}

	public long getWeixinTotalComments() {
		return weixinTotalComments;
	}

	public void setWeixinTotalComments(long weixinTotalComments) {
		this.weixinTotalComments = weixinTotalComments;
	}

	public long getPlurkTotalComments() {
		return plurkTotalComments;
	}

	public void setPlurkTotalComments(long plurkTotalComments) {
		this.plurkTotalComments = plurkTotalComments;
	}

	public long getLineqTotalComments() {
		return lineqTotalComments;
	}

	public void setLineqTotalComments(long lineqTotalComments) {
		this.lineqTotalComments = lineqTotalComments;
	}

	public long getWebPosComments() {
		return webPosComments;
	}

	public void setWebPosComments(long webPosComments) {
		this.webPosComments = webPosComments;
	}

	public long getWebNeuComments() {
		return webNeuComments;
	}

	public void setWebNeuComments(long webNeuComments) {
		this.webNeuComments = webNeuComments;
	}

	public long getWebNegComments() {
		return webNegComments;
	}

	public void setWebNegComments(long webNegComments) {
		this.webNegComments = webNegComments;
	}

	public long getFacebookPosComments() {
		return facebookPosComments;
	}

	public void setFacebookPosComments(long facebookPosComments) {
		this.facebookPosComments = facebookPosComments;
	}

	public long getFacebookNeuComments() {
		return facebookNeuComments;
	}

	public void setFacebookNeuComments(long facebookNeuComments) {
		this.facebookNeuComments = facebookNeuComments;
	}

	public long getFacebookNegComments() {
		return facebookNegComments;
	}

	public void setFacebookNegComments(long facebookNegComments) {
		this.facebookNegComments = facebookNegComments;
	}

	public long getTwitterPosComments() {
		return twitterPosComments;
	}

	public void setTwitterPosComments(long twitterPosComments) {
		this.twitterPosComments = twitterPosComments;
	}

	public long getTwitterNeuComments() {
		return twitterNeuComments;
	}

	public void setTwitterNeuComments(long twitterNeuComments) {
		this.twitterNeuComments = twitterNeuComments;
	}

	public long getTwitterNegComments() {
		return twitterNegComments;
	}

	public void setTwitterNegComments(long twitterNegComments) {
		this.twitterNegComments = twitterNegComments;
	}

	public long getWeiboPosComments() {
		return weiboPosComments;
	}

	public void setWeiboPosComments(long weiboPosComments) {
		this.weiboPosComments = weiboPosComments;
	}

	public long getWeiboNeuComments() {
		return weiboNeuComments;
	}

	public void setWeiboNeuComments(long weiboNeuComments) {
		this.weiboNeuComments = weiboNeuComments;
	}

	public long getWeiboNegComments() {
		return weiboNegComments;
	}

	public void setWeiboNegComments(long weiboNegComments) {
		this.weiboNegComments = weiboNegComments;
	}

	public long getYoutubePosComments() {
		return youtubePosComments;
	}

	public void setYoutubePosComments(long youtubePosComments) {
		this.youtubePosComments = youtubePosComments;
	}

	public long getYoutubeNeuComments() {
		return youtubeNeuComments;
	}

	public void setYoutubeNeuComments(long youtubeNeuComments) {
		this.youtubeNeuComments = youtubeNeuComments;
	}

	public long getYoutubeNegComments() {
		return youtubeNegComments;
	}

	public void setYoutubeNegComments(long youtubeNegComments) {
		this.youtubeNegComments = youtubeNegComments;
	}

	public long getInstagramPosComments() {
		return instagramPosComments;
	}

	public void setInstagramPosComments(long instagramPosComments) {
		this.instagramPosComments = instagramPosComments;
	}

	public long getInstagramNeuComments() {
		return instagramNeuComments;
	}

	public void setInstagramNeuComments(long instagramNeuComments) {
		this.instagramNeuComments = instagramNeuComments;
	}

	public long getInstagramNegComments() {
		return instagramNegComments;
	}

	public void setInstagramNegComments(long instagramNegComments) {
		this.instagramNegComments = instagramNegComments;
	}

	public long getWeixinPosComments() {
		return weixinPosComments;
	}

	public void setWeixinPosComments(long weixinPosComments) {
		this.weixinPosComments = weixinPosComments;
	}

	public long getWeixinNeuComments() {
		return weixinNeuComments;
	}

	public void setWeixinNeuComments(long weixinNeuComments) {
		this.weixinNeuComments = weixinNeuComments;
	}

	public long getWeixinNegComments() {
		return weixinNegComments;
	}

	public void setWeixinNegComments(long weixinNegComments) {
		this.weixinNegComments = weixinNegComments;
	}

	public long getPlurkPosComments() {
		return plurkPosComments;
	}

	public void setPlurkPosComments(long plurkPosComments) {
		this.plurkPosComments = plurkPosComments;
	}

	public long getPlurkNeuComments() {
		return plurkNeuComments;
	}

	public void setPlurkNeuComments(long plurkNeuComments) {
		this.plurkNeuComments = plurkNeuComments;
	}

	public long getPlurkNegComments() {
		return plurkNegComments;
	}

	public void setPlurkNegComments(long plurkNegComments) {
		this.plurkNegComments = plurkNegComments;
	}

	public long getLineqPosComments() {
		return lineqPosComments;
	}

	public void setLineqPosComments(long lineqPosComments) {
		this.lineqPosComments = lineqPosComments;
	}

	public long getLineqNeuComments() {
		return lineqNeuComments;
	}

	public void setLineqNeuComments(long lineqNeuComments) {
		this.lineqNeuComments = lineqNeuComments;
	}

	public long getLineqNegComments() {
		return lineqNegComments;
	}

	public void setLineqNegComments(long lineqNegComments) {
		this.lineqNegComments = lineqNegComments;
	}
	
	
}

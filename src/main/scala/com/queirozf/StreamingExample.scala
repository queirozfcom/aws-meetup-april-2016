package com.queirozf

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationBuilder

/**
  * Created by felipe on 16/04/16.
  */
object StreamingExample extends Logging{


  // crie suas credenciais em https://apps.twitter.com/
  val apiKey = """xxxxx"""
  val apiSecret = """xxxxx"""
  val accessToken = """xxxxx"""
  val accessTokenSecret = """xxxxx"""

  val batchDuration = Seconds(10)


  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("aws-meetup-rio-2016-streaming")
    val ssc = new StreamingContext(config,batchDuration)

    val twitterConf = new ConfigurationBuilder()
    twitterConf.setOAuthAccessToken(accessToken)
    twitterConf.setOAuthAccessTokenSecret(accessTokenSecret)
    twitterConf.setOAuthConsumerKey(apiKey)
    twitterConf.setOAuthConsumerSecret(apiSecret)


    val auth = AuthorizationFactory.getInstance(twitterConf.build())


    val tweets = TwitterUtils.createStream(ssc,Some(auth))

    val portuguesetweets = tweets
      .map(status => status.getLang)
      .filter(lang => lang == "pt" )

    portuguesetweets.count().print()

    ssc.start()
    ssc.awaitTermination()
  }

}


import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import twitter4j.{Place, TwitterFactory}
import twitter4j.auth.{AuthorizationFactory, AccessToken, Authorization}
import twitter4j.conf.ConfigurationBuilder

/**
  * Created by felipe on 26/04/16.
  */
object StreamingExample extends Logging{

//  val apiKey = """JG0nOQbH13ckyQIdZWWfeQgXd"""
//  val apiSecret = """S6m50He74GORYyTgnHB5Be5WUwHYGZe60r8En0JzLVirefRjnm"""
//  val accessToken = """261548171-4xTWn8lMbOK19RMgSVMmvfcPFCJLsFfnMI9xUKGl"""
//  val accessTokenSecret = """x5e4b9qkAVO5yx3MQqnQAxwWLF3pG9HYftxX6b0eL075l"""

  val apiKey = """xxxxx"""
  val apiSecret = """xxxxx"""
  val accessToken = """xxxxx"""
  val accessTokenSecret = """xxxxx"""

  val batchDuration = Seconds(2)


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


    val braziltweets = tweets
      .map(status => status.getPlace.getCountry)
      .filter( country => country == "Brazil" )
      .count()

    braziltweets.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

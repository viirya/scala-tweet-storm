package storm.scala.tweet

import storm.scala.dsl._
import storm.scala.dsl.FunctionalTrident._
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}

import storm.trident.tuple.TridentTuple
import storm.trident.operation.{TridentCollector, BaseFunction}
import storm.trident.TridentTopology
import storm.trident.testing.{MemoryMapState, FixedBatchSpout}
import backtype.storm.{StormSubmitter, LocalDRPC, LocalCluster, Config}
import storm.trident.operation.builtin._
import storm.trident.state.{State, QueryFunction}

import collection.mutable.{Map, HashMap}
import util.Random
import java.io.InputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.util.zip.GZIPInputStream
import java.net.SocketTimeoutException

import scala.io.Codec

import com.mongodb.casbah.Imports._

import com.streamer.twitter._
import com.streamer.twitter.config.{Config => TwitterConfig}

import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.redis._
import akka.actor.{ ActorSystem, Props }

import com.github.pmerienne.trident.ml.nlp.TwitterSentimentClassifier


class TweetStreamProcessor extends StreamProcessor {

  var reader: BufferedReader = null

  override def process(is: InputStream): Unit = {
    reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(is), "UTF-8"))
    while(reader != null) {
    }
  }

  def parseJson(json: String): Option[(Double, Double, String)] = {
    (parse(json) \ "geo" \ "coordinates") match {
      case JArray(List(JDouble(lng), JDouble(lat))) =>
        (parse(json) \ "text") match {
          case JString(text) => Some((lat, lng, text))    
        }
      case _ => None
    }
  }    

  def getLine(): Option[(Double, Double, String)] = {
    if (reader != null) {
        reader.readLine() match {
          case line: String => parseJson(line)
          case _ => None
        }
    } else {
      None
    }
  }

  def clean() = {
    reader = null
  }
}

class RunnableClient(client: BasicStreamingClient) extends Runnable {
  def run() {
    client.filter(locations = "-180,-90,180,90")
  }
}
 
class TweetStreamSpout extends StormSpout(outputFields = List("geo_lat", "geo_lng", "lat", "lng", "txt")) {

  var processor: TweetStreamProcessor = _
  var username: String = _
  var password: String = _

  var twitterClient: BasicStreamingClient = _
  var tweet_thread: Thread = _

  setup {
    processor = new TweetStreamProcessor()
    username = TwitterConfig.readString("username")
    password = TwitterConfig.readString("password")

    twitterClient = new BasicStreamingClient(username, password, processor.asInstanceOf[StreamProcessor])
    new Thread(new RunnableClient(twitterClient)).start()
  }

  def nextTuple = {
    try {
      processor.getLine() match {
        case Some((lat: Double, lng: Double, txt: String)) => emit (math.floor(lat * 10000), math.floor(lng * 10000), lat, lng, txt)
        case None =>
      }
    } catch {
      case e: SocketTimeoutException =>
        println("SocketTimeoutException")

        processor.clean()
        twitterClient = new BasicStreamingClient(username, password, processor.asInstanceOf[StreamProcessor])
        new Thread(new RunnableClient(twitterClient)).start()
    }
  }
}

class ClockSpout extends StormSpout(outputFields = List("timestamp")) {
  def nextTuple {
    Thread sleep 1000 * 1
    emit (System.currentTimeMillis / 1000)
  }
}


object Pub {
  val system = ActorSystem("pub")
  val r = new RedisClient("localhost", 6379)
  val p = system.actorOf(Props(new Publisher(r)))

  def publish(channel: String, message: String) = {
    p ! Publish(channel, message)
  }
}

class GeoGrouping extends StormBolt(List("geo_lat", "geo_lng", "lat", "lng", "txt")) {

  import nak.NakContext._
  import nak.core._
  import nak.data._
  import nak.liblinear.LiblinearConfig
  import nak.util.ConfusionMatrix

  import java.io.File


  var average_lat: Map[String, List[Double]] = _
  var average_lng: Map[String, List[Double]] = _
  var insert_time: Map[String, List[Long]] = _
  var grp_tweets: Map[String, List[String]] = _

  implicit var isoCodec: Codec = _

  val stopwords = Set("the","a","an","of","in","for","by","on")
  var featurizer: Featurizer[String, String] = _
  var classifier: IndexedClassifier[String] with FeaturizedClassifier[String, String] = null

  var tweet_classifier: TwitterSentimentClassifier = _

 
  setup {
    average_lat = new HashMap[String, List[Double]]().withDefaultValue(List())
    average_lng = new HashMap[String, List[Double]]().withDefaultValue(List())
    insert_time = new HashMap[String, List[Long]]().withDefaultValue(List())
    grp_tweets = new HashMap[String, List[String]]().withDefaultValue(List())

    setup_classifier()
  }

  def setup_classifier() = {
 
    isoCodec = scala.io.Codec("ISO-8859-1")
    featurizer = new BowFeaturizer(stopwords)
   
    val modelFile = new File(getClass.getResource("/corpus/20Newsgroups.model").getFile()) 
    val fmapFile = new File(getClass.getResource("/corpus/20Newsgroups.fmap").getFile())
    val lmapFile = new File(getClass.getResource("/corpus/20Newsgroups.lmap").getFile())

    classifier = loadClassifier(modelFile, fmapFile, lmapFile, featurizer)

    tweet_classifier = new TwitterSentimentClassifier()

  }

  def predict_tweets(tweets: String) = {

    val maxLabelNews = maxLabel(classifier.labels) _
    maxLabelNews(classifier.evalRaw(tweets))
  }

  def predict_tweets_sentiment(tweets: String): String = {
    tweet_classifier.classify(tweets) match {
      case java.lang.Boolean.TRUE => return "positive"
      case java.lang.Boolean.FALSE => return "negative"
      case _ => return "unknown"
    }
  }

  def group_publish(elem_key: String) = {
 
      var all_lat = 0.0
      var all_lng = 0.0
 
      average_lat(elem_key).foreach((lat) => all_lat += lat)
      average_lng(elem_key).foreach((lng) => all_lng += lng)

      if (average_lat(elem_key).length > 0) {
        all_lat /= average_lat(elem_key).length
        all_lng /= average_lng(elem_key).length
      }

      var concated_txt = ""
      grp_tweets(elem_key).foreach((txt) => concated_txt += " " + txt)

      val predict_label = predict_tweets(concated_txt)
      var sentiment_label = predict_tweets_sentiment(concated_txt)

      if (all_lat != 0.0 || all_lng != 0.0 || average_lat(elem_key).length == 0) {
        Pub.publish("tweets", elem_key + ":" + all_lat.toString() + ":" + all_lng.toString() + ":" + average_lat(elem_key).length + "\t" + concated_txt + "\t" + predict_label + "\t" + sentiment_label)
      }

      if (average_lat(elem_key).length == 0) {
        average_lat.remove(elem_key)
        average_lng.remove(elem_key)
        insert_time.remove(elem_key)
        grp_tweets.remove(elem_key)
      }
 
  } 

  def execute(t: Tuple) = t matchSeq {
    case Seq(clockTime: Long) =>

        val current_time = System.currentTimeMillis
        average_lat.foreach((elem) => {
          var count = 0
          average_lat(elem._1) = elem._2.filterNot((tweet) => {
            count += 1
            (current_time - insert_time(elem._1)(count - 1)) / 1000 > 5
          })
        })

        average_lng.foreach((elem) => {
          var count = 0
          average_lng(elem._1) = elem._2.filterNot((tweet) => {
            count += 1
            (current_time - insert_time(elem._1)(count - 1)) / 1000 > 5
          })
        })

        grp_tweets.foreach((elem) => {
          var count = 0
          grp_tweets(elem._1) = elem._2.filterNot((tweet) => {
            count += 1
            (current_time - insert_time(elem._1)(count - 1)) / 1000 > 5
          })
        })
 
        insert_time.foreach((elem) => {
          insert_time(elem._1) = elem._2.filterNot((record_time) => {
            (current_time - record_time) / 1000 > 5
          })
        }) 

        average_lat.foreach((elem) => {
          if (elem._2.length == 0) {
            group_publish(elem._1)
          }
        })

    case Seq(geo_lat: Double, geo_lng: Double, lat: Double, lng: Double, txt: String) =>
      
      val elem_key = geo_lat.toString() + ":" + geo_lng.toString()

      average_lat(elem_key) = lat :: average_lat(elem_key)
      average_lng(elem_key) = lng :: average_lng(elem_key)
      insert_time(elem_key) = System.currentTimeMillis :: insert_time(elem_key)
      grp_tweets(elem_key) = txt :: grp_tweets(elem_key)

      using anchor t emit (geo_lat, geo_lng, average_lat(elem_key), average_lng(elem_key))

      group_publish(elem_key)

      Pub.publish("ori_tweets", lat.toString() + ":" + lng.toString())
 
      t ack
  }
}


object TweetsStreamingTopology {

  def main(args: Array[String]) = {
    val builder = new TopologyBuilder

    builder.setSpout("tweetstream", new TweetStreamSpout, 1)
    builder.setSpout("clock", new ClockSpout)
    builder.setBolt("geogrouping", new GeoGrouping, 12)
        .fieldsGrouping("tweetstream", new Fields("geo_lat", "geo_lng"))
        .allGrouping("clock")

    val conf = new Config

    if (args.length > 0) {
        args(0) match {
            case "local" => {
                conf.setDebug(false)
                conf.setMaxTaskParallelism(3)
                
                val cluster = new LocalCluster
                cluster.submitTopology("tweets-streaming-grouping", conf, builder.createTopology)
            }
            case "remote" => {
                conf.setNumWorkers(20);
                conf.setMaxSpoutPending(5000);
                StormSubmitter.submitTopology("tweets-streaming-grouping", conf, builder.createTopology);
            }
        }
    } else {
      println("Usage: storm jar <jarfile> storm.scala.tweet.TweetsStreamingTopology <local|remote>")
    }
  }
}

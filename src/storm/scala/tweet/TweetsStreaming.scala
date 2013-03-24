package storm.scala.tweet

import storm.scala.dsl._
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}
import collection.mutable.{Map, HashMap}
import util.Random
import java.io.InputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.util.zip.GZIPInputStream

import com.mongodb.casbah.Imports._

import com.streamer.twitter._
import com.streamer.twitter.config.{Config => TwitterConfig}

import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.redis._
import akka.actor.{ ActorSystem, Props }


class TweetStreamProcessor extends StreamProcessor {

  var reader: BufferedReader = null

  override def process(is: InputStream): Unit = {
    reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(is), "UTF-8"))
    while(true) {
    }
  }

  def parseJson(json: String): Option[(Double, Double)] = {
    (parse(json) \ "geo" \ "coordinates") match {
      case JArray(List(JDouble(lng), JDouble(lat))) => Some((lat, lng))
      case _ => None
    }
  }    

  def getLine(): Option[(Double, Double)] = {
    if (reader != null) {
        reader.readLine() match {
          case line: String => parseJson(line)
          case _ => None
        }
    } else {
      None
    }
  }
}

class RunnableClient(client: BasicStreamingClient) extends Runnable {
  def run() {
    client.filter(locations = "-180,-90,180,90")
  }
}
 
class TweetStreamSpout extends StormSpout(outputFields = List("geo_lat", "geo_lng", "lat", "lng")) {

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
    processor.getLine() match {
      case Some((lat: Double, lng: Double)) => emit (math.floor(lat * 100000), math.floor(lng * 100000), lat, lng)
      case None =>
    }
  }
}

class ClockSpout extends StormSpout(outputFields = List("timestamp")) {
  def nextTuple {
    Thread sleep 1000 * 5
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

class GeoGrouping extends StormBolt(List("geo_lat", "geo_lng", "lat", "lng")) {
  var average_lat: Map[String, List[Double]] = _
  var average_lng: Map[String, List[Double]] = _
 
  setup {
    average_lat = new HashMap[String, List[Double]]().withDefaultValue(List())
    average_lng = new HashMap[String, List[Double]]().withDefaultValue(List())
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

      if (all_lat != 0.0 || all_lng != 0.0 || average_lat(elem_key).length == 0) {
        Pub.publish("tweets", elem_key + ":" + all_lat.toString() + ":" + all_lng.toString() + ":" + average_lat(elem_key).length)
      }

      if (average_lat(elem_key).length == 0) {
        average_lat.remove(elem_key)
      }
 
  } 

  def execute(t: Tuple) = t matchSeq {
    case Seq(clockTime: Long) =>
        average_lat.foreach((elem) => { average_lat(elem._1) = elem._2.dropRight(1) })
        average_lng.foreach((elem) => { average_lng(elem._1) = elem._2.dropRight(1) })

    case Seq(geo_lat: Double, geo_lng: Double, lat: Double, lng: Double) =>
      
      val elem_key = geo_lat.toString() + ":" + geo_lng.toString()

      average_lat(elem_key) = lat :: average_lat(geo_lat.toString() + geo_lng.toString())
      average_lng(elem_key) = lng :: average_lng(geo_lat.toString() + geo_lng.toString())

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
    conf.setDebug(true)
    conf.setMaxTaskParallelism(3)

    val cluster = new LocalCluster
    cluster.submitTopology("tweets-streaming-grouping", conf, builder.createTopology)
  }
}

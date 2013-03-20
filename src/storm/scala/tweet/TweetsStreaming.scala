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


class GeoGrouping extends StormBolt(List("geo_lat", "geo_lng", "lat", "lng")) {
  var average_lat: Map[String, Double] = _
  var average_lng: Map[String, Double] = _
 
  setup {
    average_lat = new HashMap[String, Double]().withDefaultValue(0.0)
    average_lng = new HashMap[String, Double]().withDefaultValue(0.0)
  }
  def execute(t: Tuple) = t matchSeq {
    case Seq(geo_lat: Double, geo_lng: Double, lat: Double, lng: Double) =>
      average_lat(geo_lat.toString() + geo_lng.toString()) += lat
      average_lng(geo_lat.toString() + geo_lng.toString()) += lng

      average_lat(geo_lat.toString() + geo_lng.toString()) /= 2.0
      average_lng(geo_lat.toString() + geo_lng.toString()) /= 2.0
 
      using anchor t emit (geo_lat, geo_lng, average_lat(geo_lat.toString() + geo_lng.toString()), average_lng(geo_lat.toString() + geo_lng.toString()))
      t ack
  }
}


object TweetsStreamingTopology {
  def main(args: Array[String]) = {
    val builder = new TopologyBuilder

    builder.setSpout("tweetstream", new TweetStreamSpout, 1)
    builder.setBolt("geogrouping", new GeoGrouping, 12)
        .fieldsGrouping("tweetstream", new Fields("geo_lat", "geo_lng"))

    val conf = new Config
    conf.setDebug(true)
    conf.setMaxTaskParallelism(3)

    val cluster = new LocalCluster
    cluster.submitTopology("tweets-streaming-grouping", conf, builder.createTopology)
  }
}

package generators

import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.time.Instant
import scala.io.Source
import scala.util.matching.Regex

package object useractivity {
  
  case class UserActivity(userId: String, activity: String, timestamp: String) {
    def getInstant: Instant = Instant.parse(timestamp)
  }

  class UserActivitySource(filePath: String, delayBetweenEventsMs: Int = 1000) 
    extends SourceFunction[UserActivity] {

    @volatile private var running = true

    private def parseJsonToUserActivity(jsonLine: String): Option[UserActivity] = {
      try {
        val userIdPattern: Regex = """"userId":\s*"([^"]+)""".r
        val activityPattern: Regex = """"activity":\s*"([^"]+)""".r
        val timestampPattern: Regex = """"timestamp":\s*"([^"]+)""".r
        
        val userId = userIdPattern.findFirstMatchIn(jsonLine).map(_.group(1))
        val activity = activityPattern.findFirstMatchIn(jsonLine).map(_.group(1))
        val timestamp = timestampPattern.findFirstMatchIn(jsonLine).map(_.group(1))
        
        (userId, activity, timestamp) match {
          case (Some(u), Some(a), Some(t)) => Some(UserActivity(u, a, t))
          case _ => None
        }
      } catch {
        case _: Exception => None
      }
    }

    override def run(ctx: SourceFunction.SourceContext[UserActivity]): Unit = {
      try {
        val source = Source.fromFile(filePath)
        val jsonContent = source.mkString
        source.close()
        
        // Simple JSON parsing - split by objects
        val jsonObjects = jsonContent.split("\\},\\s*\\{").map(_.trim)
          .map(s => if (!s.startsWith("{")) "{" + s else s)
          .map(s => if (!s.endsWith("}")) s + "}" else s)
          .filter(_.contains("userId"))
        
        jsonObjects.foreach { jsonObj =>
          if (!running) return
          
          parseJsonToUserActivity(jsonObj) match {
            case Some(activity) =>
              ctx.collect(activity)
              // Add delay between events to simulate real-time streaming
              if (delayBetweenEventsMs > 0) {
                Thread.sleep(delayBetweenEventsMs)
              }
            case None =>
              println(s"Failed to parse: $jsonObj")
          }
        }
        
      } catch {
        case e: Exception =>
          println(s"Error reading user activity data: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }
}

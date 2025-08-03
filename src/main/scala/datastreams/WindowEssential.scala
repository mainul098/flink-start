package datastreams

import org.apache.flink.streaming.api.scala._
import generators.useractivity.{UserActivity, UserActivitySource}

object WindowEssential {

  private def aggregateUserActivityStream(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Create user activity stream from JSON file
    val userActivityStream: DataStream[UserActivity] = env.addSource(
      new UserActivitySource("src/main/resources/user_activity_sample.json", 100)
    )

    // Basic processing: Print the events
    userActivityStream
      .map(activity => s"User: ${activity.userId}, Activity: ${activity.activity}, Time: ${activity.timestamp}")
      .print()

    // Execute the job
    env.execute("User Activity Stream")
  }

  def main(args : Array[String]): Unit = {
    aggregateUserActivityStream()
  }
}

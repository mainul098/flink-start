package datastreams

import generators.useractivity.{UserActivity, UserActivitySource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Flink Window Learning - A comprehensive example for learning Flink windowing features
 * 
 * This application demonstrates:
 * - Reading user activity data from JSON
 * - Processing out-of-order events
 * - Window operations and aggregations
 * - Watermark handling for late events
 */
object FlinkWindowLearning {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  // Create user activity stream from JSON file
  private val userActivitySource: DataStream[UserActivity] = env.addSource(
    new UserActivitySource("src/main/resources/user_activity_sample.json", 100)
  )

  private val userActivityStream = userActivitySource.assignTimestampsAndWatermarks(
    // extract timestamps for event (event time) + watermarks
    WatermarkStrategy
      .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))  // once you get an event with time T, you will NOT accept further events with time < T - 500
      .withTimestampAssigner(
        new SerializableTimestampAssigner[UserActivity] {
          override def extractTimestamp(element: UserActivity, recordTimestamp: Long): Long = element.getInstant.toEpochMilli
        })
  )

  // How many user are registered in every 3 seconds
  // [0...3s] [3s...6s] [6s...9s]
  private val threeSecondsTumblingWindow = userActivityStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
  /*
    |-----0-----|-----1-----|-----2----|------3------|-----4-----|-----5-----|-----6-----|-----7-----|-----8-----|-----9-----|-----10-----|
    |  batman   | superman  |          |  aquaman    |           |           | superman  | flash     |           | robin     |            |
    |  register | register  |          |  register   |           |           | register  | register  |           | register  |
    |           |           |          |             |           |           |           |           |           |           |
    ^|------------ window 1 -----------|--------------- window 2 ------------|------------ window 3 -------------|---- window 4 ----|^
    |                                  |                                     |                                   |                   |
    |           2 registrations        |           1 registration            |           2 registrations         |  1 registration   |
    |       13:00:00 - 13:00:03        |       13:00:03 - 13:00:06           |       13:00:06 - 13:00:09         |13:00:09-13:00:12  |
    */

  // Option 1: Count using AllWindowFunction
  private def countByWindowAll(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(
      new AllWindowFunction[UserActivity, String, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[UserActivity], out: Collector[String]): Unit = {
          val registeredEventCount = input.count(event => event.activity == "register")
          out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registeredEventCount")
        }
      })
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // Option 2: Count using AggregateFunction
  private def countByWindowAggregator() : Unit = {
    val registrationsPerThreeSeconds = threeSecondsTumblingWindow.aggregate(new AggregateFunction[UserActivity, Int, String] {
      override def createAccumulator(): Int = 0
      override def add(value: UserActivity, accumulator: Int): Int =
        if (value.activity == "register") accumulator + 1 else accumulator
      override def getResult(accumulator: Int): String = s"Registrations: $accumulator"
      override def merge(a: Int, b: Int): Int = a + b
    })
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // Option 2: Count using ProcessFunction
  private def countByProcessAllWindowFunction() : Unit = {
    val registrationsPerThreeSeconds = threeSecondsTumblingWindow.process(new ProcessAllWindowFunction[UserActivity, String, TimeWindow] {
      override def process(context: Context, elements: Iterable[UserActivity], out: Collector[String]): Unit = {
        val window = context.window
        val registeredEventCount = elements.count(event => event.activity == "register")
        out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registeredEventCount")
      }
    })
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  /*
  Keyed window
  */
  private val userActivityByTypeStream: KeyedStream[UserActivity, String] = userActivityStream.keyBy(e => e.activity)
  private val threeSecondTumblingWindowsByType = userActivityByTypeStream.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  private def countByTypeByWindow(): Unit = {
    val result = threeSecondTumblingWindowsByType.apply(new WindowFunction[UserActivity, String, String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[UserActivity], out: Collector[String]): Unit = {
        out.collect(s"$key: $window, ${input.size}")
      }
    })

    result.print()
    env.execute()
  }


  def main(args: Array[String]): Unit = {
//     Choose which example to run:
//     countByWindowAll()           // Original AllWindowFunction approach
//     countByWindowAggregator()       // Aggregate function approach
//    countByProcessAllWindowFunction()
    countByTypeByWindow()
  }
}

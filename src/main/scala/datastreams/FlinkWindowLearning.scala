package datastreams

import generators.useractivity.{UserActivity, UserActivitySource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

/**
 * FlinkWindowLearning - A comprehensive tutorial for Apache Flink windowing operations
 *
 * This application demonstrates four different approaches to window-based stream processing:
 *
 * 1. AllWindowFunction    - Buffers all elements, processes at once (high memory usage)
 * 2. AggregateFunction    - Incremental aggregation (memory efficient)
 * 3. ProcessFunction      - Advanced processing with window context access
 * 4. Keyed Windows        - Separate windows per key for parallel processing
 *
 * Features demonstrated:
 * ✓ Event-time processing with watermarks
 * ✓ Handling out-of-order events
 * ✓ Tumbling time windows (3-second intervals)
 * ✓ User registration counting across time windows
 * ✓ Activity type analysis using keyed streams
 *
 * Data Flow:
 * JSON File → UserActivitySource → Watermark Assignment → Windowing → Aggregation → Output
 */
object FlinkWindowLearning {
  // Flink execution environment setup
  private val flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  
  // Constants for configuration
  private val JSON_DATA_FILE_PATH = "src/main/resources/user_activity_sample.json"
  private val STREAMING_DELAY_MILLISECONDS = 100
  private val WATERMARK_LATENESS_TOLERANCE_MS = 500
  private val WINDOW_SIZE_SECONDS = 3
  
  // Create user activity data stream from JSON file
  private val rawUserActivityDataStream: DataStream[UserActivity] = flinkEnvironment.addSource(
    new UserActivitySource(JSON_DATA_FILE_PATH, STREAMING_DELAY_MILLISECONDS)
  )

  // Configure event-time processing with watermarks for handling late events
  private val userActivityStreamWithEventTime = rawUserActivityDataStream.assignTimestampsAndWatermarks(
    WatermarkStrategy
      .forBoundedOutOfOrderness(java.time.Duration.ofMillis(WATERMARK_LATENESS_TOLERANCE_MS))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[UserActivity] {
          override def extractTimestamp(userActivity: UserActivity, recordTimestamp: Long): Long = 
            userActivity.getInstant.toEpochMilli
        })
  )

  // Define tumbling windows of 3 seconds for counting user registrations
  private val threeSecondTumblingWindowForAllEvents = userActivityStreamWithEventTime
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
  /*
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
    |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |                                     |                                             |                                           |                                    |
    |            1 registrations          |               3 registrations               |              2 registration               |            0 registrations         |
    |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
 */

  /**
   * Approach 1: Count registrations using AllWindowFunction
   * This approach buffers all window elements in memory before processing
   */
  private def countRegistrationsUsingAllWindowFunction(): Unit = {
    val registrationCountPerWindow: DataStream[String] = threeSecondTumblingWindowForAllEvents.apply(
      new AllWindowFunction[UserActivity, String, TimeWindow] {
        override def apply(timeWindow: TimeWindow, allEventsInWindow: Iterable[UserActivity], outputCollector: Collector[String]): Unit = {
          val registrationEventCount = allEventsInWindow.count(userActivity => userActivity.activity == "register")
          val windowResult = s"Window [${timeWindow.getStart} - ${timeWindow.getEnd}] Registration Count: $registrationEventCount"
          outputCollector.collect(windowResult)
        }
      })
    registrationCountPerWindow.print()
    flinkEnvironment.execute("Count Registrations Using AllWindowFunction")
  }

  /**
   * Approach 2: Count registrations using AggregateFunction
   * This approach uses incremental aggregation for memory efficiency
   */
  private def countRegistrationsUsingIncrementalAggregation(): Unit = {
    val registrationCountPerWindow = threeSecondTumblingWindowForAllEvents.aggregate(
      new AggregateFunction[UserActivity, Int, String] {
        override def createAccumulator(): Int = 0
        
        override def add(userActivity: UserActivity, currentCount: Int): Int =
          if (userActivity.activity == "register") currentCount + 1 else currentCount
        
        override def getResult(finalCount: Int): String = s"Total Registrations: $finalCount"
        
        override def merge(count1: Int, count2: Int): Int = count1 + count2
      })
    registrationCountPerWindow.print()
    flinkEnvironment.execute("Count Registrations Using Incremental Aggregation")
  }

  /**
   * Approach 3: Count registrations using ProcessAllWindowFunction
   * This approach provides access to window context and state management
   */
  private def countRegistrationsUsingProcessFunction(): Unit = {
    val registrationCountWithWindowInfo = threeSecondTumblingWindowForAllEvents.process(
      new ProcessAllWindowFunction[UserActivity, String, TimeWindow] {
        override def process(windowContext: Context, allWindowElements: Iterable[UserActivity], outputCollector: Collector[String]): Unit = {
          val currentWindow = windowContext.window
          val registrationEventCount = allWindowElements.count(userActivity => userActivity.activity == "register")
          val detailedResult = s"Window [${currentWindow.getStart} - ${currentWindow.getEnd}] Registration Count: $registrationEventCount"
          outputCollector.collect(detailedResult)
        }
      })
    registrationCountWithWindowInfo.print()
    flinkEnvironment.execute("Count Registrations Using ProcessFunction")
  }

  /* ================
     Keyed Windows
     ===============
  */

  // Separate windows for each activity type
  private val userActivityStreamKeyedByActivityType: KeyedStream[UserActivity, String] = 
    userActivityStreamWithEventTime.keyBy(userActivity => userActivity.activity)
  
  private val threeSecondTumblingWindowsGroupedByActivityType = 
    userActivityStreamKeyedByActivityType.window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))

  /*
    === Registration Events Stream ===
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |               |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |            1 registration           |               3 registrations               |              2 registrations              |            0 registrations         |
    |     1643760000000 - 1643760003000   |        1643760003000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |

    === Online Events Stream ===
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob online      |                 | sam online        |       | mary online     |       |                 |     | rob online    | carl online  |
    |         |         |                 |                 |                   |       |                 |       |                 |     | alice online  |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |            1 online                 |               1 online                      |              1 online                     |            3 online                |
    |     1643760000000 - 1643760003000   |        1643760003000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
  */
  /**
   * Count events per activity type using Keyed Windows
   * This creates separate windows for each activity type (register, online, etc.)
   */
  private def countEventsByActivityTypeUsingKeyedWindows(): Unit = {
    val eventCountPerActivityTypePerWindow = threeSecondTumblingWindowsGroupedByActivityType.apply(
      new WindowFunction[UserActivity, String, String, TimeWindow] {
        override def apply(activityType: String, timeWindow: TimeWindow, eventsInWindow: Iterable[UserActivity], outputCollector: Collector[String]): Unit = {
          val eventCount = eventsInWindow.size
          val result = s"Activity Type: $activityType, Window: [${timeWindow.getStart} - ${timeWindow.getEnd}], Event Count: $eventCount"
          outputCollector.collect(result)
        }
      })

    eventCountPerActivityTypePerWindow.print()
    flinkEnvironment.execute("Count Events by Activity Type Using Keyed Windows")
  }

  /*
  ===============
  Sliding Windows
  ===============
   */

  // How many user were registered every 3 seconds, UPDATED EVERY 1s
  // [0s...3s] [1s...4s] [2s...5s] ...

  /*
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    | carl online  |
  |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
  ^|------------ window one ----------- +
                1 registration

            + ---------------- window two --------------- +
                                  2 registrations

                       + ------------------- window three ------------------- +
                                           4 registrations

                                         + ---------------- window four --------------- +
                                                          3 registrations

                                                          + ---------------- window five -------------- +
                                                                           3 registrations

                                                                               + ---------- window six -------- +
                                                                                           1 registration

                                                                                        + ------------ window seven ----------- +
                                                                                                    2 registrations

                                                                                                         + ------- window eight------- +
                                                                                                                  1 registration

                                                                                                                + ----------- window nine ----------- +
                                                                                                                        1 registration

                                                                                                                                   + ---------- window ten --------- +
                                                                                                                                              0 registrations
   */

  private def countRegistrationsOnSlidingWindow(): Unit = {
    val slidingWindow = userActivityStreamWithEventTime.windowAll(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(1)))
    val result =  slidingWindow.apply(new AllWindowFunction[UserActivity, String, TimeWindow] {
      override def apply(window: TimeWindow, input: Iterable[UserActivity], out: Collector[String]): Unit = {
        val count = input.count(event => event.activity == "register")
        out.collect(s"Window [${window.getStart} - ${window.getEnd}] $count")
      }
    })

    result.print()
    flinkEnvironment.execute()
  }

  /*
  ===============
    GLobal Window
  ===============
   */
  // How many registration events do we have in every 5 events
  // NOTE: It not bounded by time

  private def countRegistrationWithEventCount(): Unit = {
    val globalWindowEvents = rawUserActivityDataStream
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of[GlobalWindow](5))
      .apply(new AllWindowFunction[UserActivity, String, GlobalWindow] {
        override def apply(window: GlobalWindow, input: Iterable[UserActivity], out: Collector[String]): Unit = {
          val eventCount = input.count(event => event.activity == "register")
          out.collect(s"Window [$window] $eventCount")
        }
      })

    globalWindowEvents.print()
    flinkEnvironment.execute()
  }

  /**
   * Main method - Choose which windowing approach to demonstrate
   */
  def main(args: Array[String]): Unit = {
    // Choose which windowing approach to run:
    
    // countRegistrationsUsingAllWindowFunction()         // Approach 1: AllWindowFunction (buffers all elements)
    // countRegistrationsUsingIncrementalAggregation()    // Approach 2: AggregateFunction (incremental)
    // countRegistrationsUsingProcessFunction()           // Approach 3: ProcessFunction (with context)
    // countEventsByActivityTypeUsingKeyedWindows()       // Keyed Windows (separate per activity type)
    // countRegistrationsOnSlidingWindow()                 // Sliding Windows
    countRegistrationWithEventCount()
  }
}

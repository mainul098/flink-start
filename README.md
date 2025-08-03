# Flink Learning Project

This project is designed as a learning tool for Apache Flink using Scala. It focuses on understanding and experimenting with Flink's core features, particularly window operations.

## Project Structure

- **src/main/scala/generators/useractivity**: Contains a data generator that reads user activity data from a JSON file and streams it into Flink.
- **src/main/scala/datastreams/FlinkWindowLearning.scala**: Demonstrates the use of Flink windowing features on streaming data.

## Features

- **Read User Activity Data**: Parses sample user activity data from JSON.
- **Flink Window Example**: Illustrates how to apply window functions to handle time-based data aggregations.
- **Process Out-of-Order Data**: Incorporates handling for late-arriving data within the stream processing, enhancing the learning of watermark concepts.

## Dependencies

- Apache Flink
- Scala
- sbt

## How to Run

1. Ensure all dependencies are installed and up-to-date.
2. Use sbt to compile and run the Flink job:
   ```bash
   sbt runMain datastreams.FlinkWindowLearning
   ```
3. The output will display processed user activity events.

## Future Learning Goals

- Experiment with different types of windows (tumbling, sliding, session windows).
- Explore watermark customization and handling late data.
- Implement additional Flink features as learning progresses.

Designed for educational purposes, this project seeks to provide a hands-on Flink experience. Explore further by expanding window operations and applying learned techniques to more complex scenarios.

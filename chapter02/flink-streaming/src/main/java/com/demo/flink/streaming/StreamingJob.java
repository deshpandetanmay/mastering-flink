package com.demo.flink.streaming;

import java.util.Properties;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.enableCheckpointing(5000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");

		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09<>("temp", new SimpleStringSchema(),
				properties);
		myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());


		DataStream<Tuple2<String, Double>> keyedStream = env.addSource(myConsumer).flatMap(new Splitter()).keyBy(0)
				.timeWindow(Time.seconds(300))
				.apply(new WindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple, TimeWindow>() {

					@Override
					public void apply(Tuple key, TimeWindow window, Iterable<Tuple2<String, Double>> input,
							Collector<Tuple2<String, Double>> out) throws Exception {
						double sum = 0L;
						int count = 0;
						for (Tuple2<String, Double> record : input) {
							sum += record.f1;
							count++;
						}

						Tuple2<String, Double> result = input.iterator().next();
						result.f1 = (sum/count);
						out.collect(result);

					}
				});

		keyedStream.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

}

package com.demo.chapter05;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

public class KafkaApp {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		DataStream<TemperatureEvent> inputEventStream = env.addSource(
				new FlinkKafkaConsumer09<TemperatureEvent>("test", new EventDeserializationSchema(), properties));

		Pattern<TemperatureEvent, ?> warningPattern = Pattern.<TemperatureEvent> begin("first")
				.subtype(TemperatureEvent.class).where(new FilterFunction<TemperatureEvent>() {
					private static final long serialVersionUID = 1L;

					public boolean filter(TemperatureEvent value) {
						if (value.getTemperature() >= 26.0) {
							return true;
						}
						return false;
					}
				}).within(Time.seconds(10));

		DataStream<Alert> patternStream = CEP.pattern(inputEventStream, warningPattern)
				.select(new PatternSelectFunction<TemperatureEvent, Alert>() {
					private static final long serialVersionUID = 1L;

					public Alert select(Map<String, TemperatureEvent> event) throws Exception {

						return new Alert("Temperature Rise Detected:" + event.get("first").getTemperature()
								+ " on machine name:" + event.get("first").getMachineName());
					}

				});

		patternStream.print();
		env.execute("CEP on Temperature Sensor");
	}
}

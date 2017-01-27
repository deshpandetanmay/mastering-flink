package com.demo.flink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Implements the Oylympics Athletes program that gives insights about games
 * played and medals won.
 * 
 * Sample input file is provided in src/main/resources/data folder
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink batch program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class OlympicsAthletesBatchJob {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().registerTypeWithKryoSerializer(Record.class, RecordSerializer.class);

		DataSet<Record> csvInput = env
				.readCsvFile("D://NOTBACKEDUP//dataflow//flink-batch//src//main//resources//data//olympic-athletes.csv")
				.pojoType(Record.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");

		DataSet<Tuple2<String, Integer>> groupedByCountry = csvInput
				.flatMap(new FlatMapFunction<Record, Tuple2<String, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(Record record, Collector<Tuple2<String, Integer>> out) throws Exception {

						out.collect(new Tuple2<String, Integer>(record.getCountry(), 1));
					}
				}).groupBy(0).sum(1);
		groupedByCountry.print();

		DataSet<Tuple2<String, Integer>> groupedByGame = csvInput
				.flatMap(new FlatMapFunction<Record, Tuple2<String, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(Record record, Collector<Tuple2<String, Integer>> out) throws Exception {

						out.collect(new Tuple2<String, Integer>(record.getGame(), 1));
					}
				}).groupBy(0).sum(1);
		groupedByGame.print();

		// Get a data set to be broadcasted
		DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);
		DataSet<String> data = env.fromElements("India", "USA", "UK").map(new RichMapFunction<String, String>() {
			private List<Integer> toBroadcast;

			// We have to use open method to get broadcast set from the context
			@Override
			public void open(Configuration parameters) throws Exception {
				// Get the broadcast set, available as collection
				this.toBroadcast = getRuntimeContext().getBroadcastVariable("country");
			}

			@Override
			public String map(String input) throws Exception {

				int sum = 0;
				for (int a : toBroadcast) {
					sum = a + sum;
				}
				return input.toUpperCase() + sum;
			}
		}).withBroadcastSet(toBroadcast, "country"); // Broadcast the set with
														// name
		data.print();

	}

}

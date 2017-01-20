package com.demo.flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;

public class BatchJob {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Record> csvInput = env
				.readCsvFile("D://NOTBACKEDUP//dataflow//flink-table//src//main//resources//data//olympic-athletes.csv")
				.pojoType(Record.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");
		// register the DataSet athletes as table "athletes" with fields derived
		// from the dataset
		Table atheltes = tableEnv.fromDataSet(csvInput);
		tableEnv.registerTable("athletes", atheltes);
		// run a SQL query on the Table and retrieve the result as a new Table
		Table groupedByCountry = tableEnv.sql("SELECT country, SUM(total) as frequency FROM athletes group by country");

		DataSet<Result> result = tableEnv.toDataSet(groupedByCountry, Result.class);

		result.print();

		Table groupedByGame = atheltes.groupBy("game").select("game, total.sum as frequency");

		DataSet<GameResult> gameResult = tableEnv.toDataSet(groupedByGame, GameResult.class);

		gameResult.print();

	}

	public static class Result {
		public String country;
		public Integer frequency;

		public Result() {
			super();
		}

		public Result(String country, Integer total) {
			this.country = country;
			this.frequency = total;
		}

		@Override
		public String toString() {
			return "Result " + country + " " + frequency;
		}
	}

	public static class GameResult {
		public String game;
		public Integer frequency;

		public GameResult(String game, Integer frequency) {
			super();
			this.game = game;
			this.frequency = frequency;
		}

		public GameResult() {
			super();
		}

		@Override
		public String toString() {
			return "GameResult [game=" + game + ", frequency=" + frequency + "]";
		}

	}
}

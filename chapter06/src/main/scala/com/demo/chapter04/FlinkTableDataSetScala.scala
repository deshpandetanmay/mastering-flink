package com.demo.chapter04

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.TableEnvironment

object FlinkTableScala {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val input = env.fromElements(WordCount("hello", 1), WordCount("hello", 1), WordCount("world", 1), WordCount("hello", 1))
    tEnv.registerDataSet("WordCount", input, 'word, 'frequency)

    // run a SQL query on the Table and retrieve the result as a new Table
    val table = tEnv.sql("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")

    table.toDataSet[WordCount].print()
  }

  case class WordCount(word: String, frequency: Long)
}
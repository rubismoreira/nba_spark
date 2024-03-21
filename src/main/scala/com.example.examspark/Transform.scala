package com.example.examspark

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, rank, sum}

import java.nio.file.{Files, Paths}

object Transform {
    
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("nbapi").master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()

    val matches = CompressDirectoryIntoDataframe(spark, "resources/matches")

    val matches_clean = matches.select(col("id"), col("date"), col("home_team_score"), col("visitor_team_score"),
      col("home_team.id").as("home_team_id"), col("home_team.full_name").as("home_team_name")
      , col("visitor_team.id").as("visitor_team_id"), col("visitor_team.full_name").as("visitor_team_name")).distinct()

    val stats_groupped: DataFrame = statusTransformer(spark)

    val result = matches_clean.join(stats_groupped, col("id") === col("game_id")).select("date", "home_team_name", "visitor_team_name", "home_team_score", "visitor_team_score", "top_scorer", "turnover_sum", "blocks_sum", "assists_sum", "persona_fouls_sum", "steals_sum")
      .na.drop(Array("top_scorer")).orderBy(col("date"))

    result.write.option("header", "true").csv("resources/result")
  }

  private def statusTransformer(spark: SparkSession): DataFrame = {
    val stats = CompressDirectoryIntoDataframe(spark, "resources/stats")

    val stats_clean = stats.select(col("id"), col("turnover"), col("blk").as("blocks"), col("min").as("minutes"), col("ast").as("assists"), col("pts").as("points"), col("pf").as("persona_fouls"), col("stl").as("steals"),
      col("game.id").as("game_id"), col("team.id").as("team_id"),
      col("player.id").as("player_id"),
      concat_ws(" ", col("player.first_name"), col("player.last_name")).as("player_full_name")).where(col("team.id").isin(24, 1, 14, 17))

    val windowSpec = Window.partitionBy("game_id", "team_id").orderBy(col("points").desc)
    val rankedPlayers = stats_clean.withColumn("rank", rank().over(windowSpec)).where(col("rank") === 1).select("game_id", "team_id", "player_full_name")

    val stats_groupped = stats_clean.groupBy("game_id", "team_id").
      agg(sum("turnover").as("turnover_sum"),
        sum("blocks").as("blocks_sum"),
        sum("assists").as("assists_sum"),
        sum("persona_fouls").as("persona_fouls_sum"),
        sum("steals").as("steals_sum")
      ).join(rankedPlayers, Seq("game_id", "team_id")).withColumnRenamed("player_full_name", "top_scorer")

    stats_groupped
  }

  private def CompressDirectoryIntoDataframe(spark: SparkSession, directory: String): sql.DataFrame = {
    val dirPath = Paths.get(directory)
    val fileNames = Files.list(dirPath).toArray.map(_.toString)

    var df = spark.read.json(fileNames.head)

    for (fileName <- fileNames.tail) {
      val currentDf = spark.read.json(fileName)
      df = df.union(currentDf)
    }

    df
  }
}

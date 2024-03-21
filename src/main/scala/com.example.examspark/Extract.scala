package com.example.examspark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer

object Extract {

  def processTeamResponse(cursor: Int): Unit = {
    val url = s"http://api.balldontlie.io/v1/games?seasons[]=2023&?team_ids[]=24&team_ids[]=1&team_ids[]=14&team_ids[]=17&per_page=25&cursor=${cursor}"

    val r = requests.get(url, headers = Map("Authorization" -> "b5b72224-9a91-4c48-af71-780e1f0cd09c"))

    val json = ujson.read(r.text())

    val next_page = json("meta").obj.get("next_cursor") match {
      case Some(next_cursor) => next_cursor.num.toInt
      case None => -1
    }

    val dataStr = json("data").toString()
    // Define the path where you want to save the file
    val path = Paths.get(s"resources/matches/${cursor}.json")

    // Write the 'data' string to the file

    Files.write(path, dataStr.getBytes(StandardCharsets.UTF_8))

    if (next_page != -1) {
      processTeamResponse(next_page)
    }

    val matchIds = json("data").arr.map(x => x.obj("id").num.toInt)
    processStatsResponses(matchIds, 0)
  }

  def processStatsResponses(matchIds: ArrayBuffer[Int], cursor: Int): Unit = {
    val matchIdsQuery = matchIds.map(id => s"game_ids[]=$id").mkString("&")
    val url = s"http://api.balldontlie.io/v1/stats?${matchIdsQuery}&per_page=100&cursor=${cursor}"
    val next_page = HandleApiGet(url, cursor, "resources/stats/stats")

    if (next_page != -1) {
      processStatsResponses(matchIds, next_page)
    }
  }

  private def HandleApiGet(url: String, cursor: Int, filePath: String): Int = {
    try {

      val r = requests.get(url, headers = Map("Authorization" -> "b5b72224-9a91-4c48-af71-780e1f0cd09c"))
      val json = ujson.read(r.text())

      val next_page = json("meta").obj.get("next_cursor") match {
        case Some(next_cursor) => next_cursor.num.toInt
        case None => -1
      }

      val dataStr = json("data")
      val path = Paths.get(s"${filePath}_$cursor.json")
      Files.write(path, dataStr.toString().getBytes(StandardCharsets.UTF_8))

      next_page
    }
    catch {
      case e: requests.RequestFailedException =>
        if(e.response.statusCode == 429){
          println("Rate limit exceeded, waiting 5 seconds")
          Thread.sleep(5000)
          return cursor
        }
        else{
          println(e)
          return  -1
        }
    }
  }

  def main(args: Array[String]) {
    Extract.processTeamResponse(0)
  }
}

# nba_spark
Playing around with spark using the nba api

# Running 

To run the project on the root run:
1. ``` sbt compile ```
2. ``` sbt run ```
    when prompted choose 1 for extract and 2 for transform -> run extract first if the download was not done. The resulting jsons will be on resources/matches and resources/stats

    ![alt text](image.png)
3. ``` sbt run ``` and choose extract -> the resulting json will be on resources/result


## Results
![alt text](image-1.png)

# Requirements
Use java 8 


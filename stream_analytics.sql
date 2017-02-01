WITH HDB04 AS (
  SELECT
        *,
       
       LAG([EventProcessedUtcTime]) OVER (LIMIT DURATION(hour, 3)) as previousTime,
       LAG([taskLocation]) OVER (LIMIT DURATION(hour, 3)) as formertaskLocation
  FROM
      hdbinput TIMESTAMP BY EventProcessedUtcTime
      WHERE name = 'movement' AND taskName = 'movement' 
   
      )
  SELECT 
  
      System.TimeStamp AS Endtime,
       
      SUM(DATEDIFF(second, previousTime, EventProcessedUtcTime)) AS timegap
      FROM HDB04 
      WHERE
        [taskLocation] = 'Bathroom' AND formertaskLocation = 'Bathroom' AND address = '01:84:D6:E0:00:05'
        AND DATEDIFF(second, previousTime, EventProcessedUtcTime) > 10
    GROUP BY TumblingWindow(minute, 1)
    

SELECT testlocation(h.tasklocation)
FROM hdbinput h
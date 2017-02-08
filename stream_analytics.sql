WITH adventis01 AS (
  SELECT
       IoTHub.ConnectionDeviceId AS hubid,
       EventProcessedUtcTime AS eventtime,
       taskLocation,
       LAG([EventProcessedUtcTime]) OVER (LIMIT DURATION(hour, 3)) as previousTime,
       LAG([taskLocation]) OVER (LIMIT DURATION(hour, 3)) as formertaskLocation
  FROM
      hdbinput TIMESTAMP BY EventProcessedUtcTime
      WHERE name = 'movement' AND taskName = 'movement' 
)

SELECT 
      tasklocation,
      hubid, 
      System.TimeStamp AS Endtime, 
      SUM(DATEDIFF(second, previousTime, EventProcessedUtcTime)) AS timegap
      FROM adventis01 
      WHERE
        [taskLocation] = 'Bathroom' AND formertaskLocation = 'Bathroom' 
        AND DATEDIFF(second, previousTime, EventProcessedUtcTime) > 10
    GROUP BY TumblingWindow(minute, 1)
    

GROUP BY
        IoTHub.ConnectionDeviceId
      
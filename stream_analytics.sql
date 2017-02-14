WITH fivemintable AS(
 SELECT
        System.TimeStamp AS eventtime,
        IoTHub.ConnectionDeviceId AS hubid,
        taskLocation,
        taskName,
        name,
        address,
        value
    FROM
        gateway TIMESTAMP BY EventProcessedUtcTime
    WHERE
        (name = 'movement' AND taskName = 'movement' ) 
        OR (name = 'heaartbeat')
)

SELECT 
    name,
    hubid,
    eventtime,
    LAG([eventtime]) OVER (PARTITION BY hubid LIMIT DURATION(hour, 3)) as previousTime,
    LAG([taskLocation],4) OVER (PARTITION BY hubid LIMIT DURATION(hour,3 )) as formertaskLocation,
    LAG([name],3) OVER (PARTITION BY hubid LIMIT DURATION(hour, 3)) as preprepreName,
    LAG([name],2) OVER (PARTITION BY hubid LIMIT DURATION(hour, 3)) as prepreName,
    LAG([name],1) OVER (PARTITION BY hubid LIMIT DURATION(hour, 3)) as preName
      
FROM fivemintable 
WHERE name = 'movement' 
      AND LAG([taskLocation],4) OVER (PARTITION BY hubid LIMIT DURATION(hour, 3)) = 'Bedroom'
      AND LAG(name,3) OVER (PARTITION BY hubid LIMIT DURATION(hour, 3)) = 'heartbeat'
      AND LAG(name,2) OVER (PARTITION BY hubid LIMIT DURATION(hour, 3)) = 'heartbeat'
      AND LAG(name,1) OVER (PARTITION BY hubid LIMIT DURATION(hour, 3)) = 'heartbeat'
       
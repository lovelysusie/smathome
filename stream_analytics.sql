WITH fivemintable AS(
 SELECT
        System.TimeStamp AS eventtime,
        IoTHub.ConnectionDeviceId AS hubid,
        taskLocation,
        taskName,
        name,
        address,
        COUNT(value) AS 'value'
    FROM
        gateway TIMESTAMP BY EventProcessedUtcTime
    WHERE
        name = 'movement' AND taskName = 'movement' AND IoTHub.ConnectionDeviceId = 'SG-04-HDB00013'

    GROUP BY
        IoTHub.ConnectionDeviceId,
        taskLocation,
        taskName,
        name,
        address,
        TumblingWindow(minute, 1)
)

SELECT 
    hubid,
    eventtime,
    LAG([eventtime]) OVER (LIMIT DURATION(minute, 2)) as previousTime,
    LAG([taskLocation]) OVER (LIMIT DURATION(minute,2 )) as formertaskLocation,
    taskLocation
FROM fivemintable 
WHERE [taskLocation] = 'Living Room' AND LAG([taskLocation]) OVER (LIMIT DURATION(minute, 2)) <> 'Bathroom'
       --AND LAG([taskLocation]) OVER (LIMIT DURATION(minute, 2)) <> 'Bedroom'
       AND LAG([taskLocation]) OVER (LIMIT DURATION(minute, 2)) <> 'Living Room'
       --AND LAG([taskLocation]) OVER (LIMIT DURATION(minute, 2)) <> 'Kitchen'
       --AND DATEDIFF(minute, LAG([eventtime]) OVER (LIMIT DURATION(minute, 2)), eventtime) > 1

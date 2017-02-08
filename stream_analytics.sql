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
        name = 'movement' AND taskName = 'movement'
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
    taskLocation,
    eventtime,
    LAG([eventtime]) OVER (LIMIT DURATION(hour, 3)) as previousTime,
    LAG([taskLocation]) OVER (LIMIT DURATION(hour, 3)) as formertaskLocation
FROM fivemintable 
WHERE [taskLocation] = 'Bathroom' AND LAG([taskLocation]) OVER (LIMIT DURATION(hour, 3)) = 'Bathroom' 
       AND DATEDIFF(minute, LAG([eventtime]) OVER (LIMIT DURATION(hour, 3)), eventtime) > 1


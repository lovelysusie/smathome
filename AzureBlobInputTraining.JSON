{
    "name": "AzureBlobInputTraining",
    "properties": {
        "published": false,
        "type": "AzureBlob",
        "linkedServiceName": "AzureStorageLinkedService",
        "typeProperties": {
            "fileName": "{Year}-{Month}-{Day}.csv",
            "folderPath": "rspark/adfinput/{Year}-{Month}-{Day}/",
            "format": {
                "type": "TextFormat",
                "rowDelimiter": "\n",
                "columnDelimiter": ","
            },
            "partitionedBy": [
                {
                    "name": "Year",
                    "value": {
                        "type": "DateTime",
                        "date": "SliceStart",
                        "format": "yyyy"
                    }
                },
                {
                    "name": "Month",
                    "value": {
                        "type": "DateTime",
                        "date": "SliceStart",
                        "format": "MM"
                    }
                },
                {
                    "name": "Day",
                    "value": {
                        "type": "DateTime",
                        "date": "SliceStart",
                        "format": "dd"
                    }
                }
            ]
        },
        "availability": {
            "frequency": "Minute",
            "interval": 30
        },
        "external": true,
        "policy": {
            "externalData": {
                "retryInterval": "04:00:00",
                "retryTimeout": "05:00:00",
                "maximumRetry": 3
            }
        }
    }
}
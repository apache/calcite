[
  {
    "dataSchema": {
      "dataSource": "foodmart",
      "parser": {
        "parseSpec": {
          "format": "json",
          "timestampSpec": {
            "column": "the_date",
            "format": "iso"
          },
          "dimensionsSpec": {
            "dimensions": [
              "gender",
              "country",
              "state_province"
            ],
            "dimensionExclusions": [

            ],
            "spatialDimensions": [
            ]
          }
        }
      },
      "metricsSpec": [
        {
          "name": "sales",
          "type": "count"
        },
        {
          "fieldName": "unit_sales",
          "name": "unit_sales",
          "type": "doubleSum"
        },
        {
          "fieldName": "store_sales",
          "name": "store_sales",
          "type": "doubleSum"
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE"
      }
    },
    "ioConfig": {
      "type": "realtime",
      "firehose": {
        "type": "local",
        "filter": "*.json",
        "baseDir": "foodmart/data"
      },
      "plumber": {
        "type": "realtime",
        "rejectionPolicy": {
          "type": "test"
         }
      }
    },
    "tuningConfig": {
        "type" : "realtime",
        "maxRowsInMemory": 500000,
        "intermediatePersistPeriod": "P10000d",
        "windowPeriod": "P10000d",
        "basePersistDirectory": "\/tmp\/realtime\/basePersist",
        "rejectionPolicy": {
            "type": "serverTime"
        }
    }
  }
]

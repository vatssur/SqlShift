# SQL Shift(MySQL To Redshift Data Transfer)

This program will transfer tables from **MySQL like databases** to Redshift.

## Usage

You can build using **SBT** and create fat jar using `sbt assembly`.
Need table details with mysql, redshift & s3 configuration in json format.
Json schema is :-

```JSON
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "array",
  "items": {
    "type": "object",
    "description": "List of mapping from mysql to redshift",
    "properties": {
      "mysql": {
        "type": "object",
        "description": "MySQL Configuration",
        "properties": {
          "db": {
            "description": "database name",
            "type": "string"
          },
          "hostname": {
            "type": "string"
          },
          "portno": {
            "type": "integer"
          },
          "username": {
            "type": "string"
          },
          "password": {
            "type": "string"
          }
        },
        "required": [
          "db",
          "hostname",
          "portno",
          "username",
          "password"
        ]
      },
      "tables": {
        "description": "Tables information list",
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "name": {
              "description": "Table name",
              "type": "string"
            },
            "incremental": {
              "description": "Incremental condition",
              "type": "string"
            },
            "mergeKey": {
              "description": "Key(column) on which duplicates are to be removed",
              "type": "string"
            },
            "addColumn": {
              "description": "Add column condition(Redshift compatible)",
              "type": "string"
            },
            "partitions": {
              "description": "Number of partitions to set(default: will work if primary key is integer and isSplittable is true",
              "type": "integer"
            },
            "isSplittable": {
              "description": "Should do partitions",
              "type": "boolean"
            }
          },
          "required": [
            "name"
          ]
        }
      },
      "redshift": {
        "type": "object",
        "properties": {
          "schema": {
            "description": "redshift schema name",
            "type": "string"
          },
          "hostname": {
            "type": "string"
          },
          "portno": {
            "type": "integer"
          },
          "username": {
            "type": "string"
          },
          "password": {
            "type": "string"
          }
        },
        "required": [
          "schema",
          "hostname",
          "portno",
          "username",
          "password"
        ]
      },
      "s3": {
        "type": "object",
        "properties": {
          "location": {
            "description": "Bucket location in temporary data to kept",
            "type": "string"
          },
          "accessKey": {
            "description": "S3 access key",
            "type": "string"
          },
          "secretKey": {
            "description": "Secret key",
            "type": "string"
          }
        },
        "required": [
          "location",
          "accessKey",
          "secretKey"
        ]
      }
    },
    "required": [
      "mysql",
      "tables",
      "redshift",
      "s3"
    ]
  }
}
```

- Mail configuration for alerting :-

```PROPERTIES
alert.host =
alert.to =
alert.cc =
alert.subject =
```

- Submitting spark job on yarn :-
```
spark-submit \
--class com.goibibo.sqlshift.SQLShift \
--master yarn \
mysql-redshift-loader-assembly-0.1.jar -td table-details.json -mail mail.conf
```

- More Options

```text
RDS to Redshift DataPipeline
Usage: Main [options]

  -td, --table-details <path to Json>
                           Table details json file path including
  -mail, --mail-details <path to properties file>
                           Mail details property file path(For enabling mail)
  -aof, --alert-on-failure
                           Alert only when fails
  -rc, --retry-count <count>
                           How many times to retry on failed transfers
  -lmr, --log-metrics-reporter
                           Enable metrics reporting in logs
  -jmr, --jmx-metrics-reporter
                           Enable metrics reporting through JMX
  -mws, --metrics-window-size <value>
                           Metrics window size in seconds. Default: 5 seconds
  --help                   Usage Of arguments
```
# mysql-redshift-loader

This program will transfer tables from MySQL to Redshift.

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

Mail configuration for alerting :-

```PROPERTIES
alert.host =
alert.to =
alert.cc =
```

Submitting spark job on yarn :-
```
spark-submit \
--class com.goibibo.mysqlRedshiftLoader.Main \
--master yarn \
--packages "org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-redshift_2.10:1.1.0,com.amazonaws:aws-java-sdk:1.7.4,mysql:mysql-connector-java:5.1.39" \
--jars="RedshiftJDBC4-1.1.17.1017.jar,mysql-redshift-loader_2.10-0.1.jar"  \
mysql-redshift-loader-assembly-0.1.jar -td table-details.json -mail mail.conf
```
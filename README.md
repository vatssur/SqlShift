# SQL Shift(MySQL To Redshift Data Transfer)

This program will transfer tables from **MySQL like databases** to Redshift.

Requirement
-----------
1. Apache Spark
2. Mysql Or Amazon RDS
3. Amazon S3
4. Amazon Redshift

Usage
-----

### Build
Build project using **SBT** and create fat jar using `sbt assembly`.

### Run
This is spark application and requires platform which can run spark on it like yarn, spark-cluster & mesos.

- Submitting spark job on yarn :-
```bash
spark-submit \
--class com.goibibo.sqlshift.SQLShift \
--master yarn \
sqlshift-assembly-0.1.jar -td table-details.json
```

- Submitting spark job locally :-
```bash
spark-submit \
--class com.goibibo.sqlshift.SQLShift \
--master local[*] \
sqlshift-assembly-0.1.jar -td table-details.json
```

- More Options

```text
MySQL to Redshift DataPipeline
Usage: SQLShift [options]

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

### Configurations

- Table configuration :- Json schema can found [here](src/main/resources/json.schema). Basic template is :-

```json
[
  {
    "mysql":
    {
        "db": "",
        "hostname": "",
        "portno": "",
        "username": "",
        "password": ""
    },
     "tables": [
     {
         "name": "",
         "incremental": "",
         "mergeKey": "",
         "addColumn": "",
         "partitions": "",
         "isSplittable": ""
     }
    ],
    "redshift":
    {
        "schema": "",
        "hostname": "",
        "portno": "",
        "username": "",
        "password": ""
    },
    "s3":
    {
        "location": "",
        "accessKey": "",
        "secretKey": ""
    }
  }
]
```

- Mail configuration for alerting :-

```PROPERTIES
alert.host =
alert.port = 25
alert.username =
alert.password =
alert.to =
alert.cc =
alert.subject =
```

## Example Configurations

1. **Full Dump**:- Let's say local mysql server is running on `localhost` with listening port `3306`. Server has user `root` with 
`admin` as password. Now we have to migrate complete table `test` from `default` database to redshift in schema `base` 
which is running on remote host.
 
```json
[
  {
    "mysql": {
      "db": "default",
      "hostname": "localhost",
      "portno": 3306,
      "username": "root",
      "password": "admin"
    },
    "tables": [
      {
        "name": "test"
      }
    ],
    "redshift": {
      "schema": "base",
      "hostname": "redshift.amazonaws.com",
      "portno": 5439,
      "username": "admin",
      "password": "admin"
    },
    "s3": {
      "location": "s3a://bucket/",
      "accessKey": "access key",
      "secretKey": "secret key"
    }
  }
]
```

**Note[1]**- Full dump of tables is good but if table is too large(like in 10s of GBs) then it can take time. So if you 
want to lift small part of table then you can do it using incremental facility provided. 
 
 
2. **Incremental**:- In incremental just add `where` condition without `where` clause in configuration. Let's say table `test` has 
`modified_at` column which is timestamp, changes when an entry/record is updated or created and we need data of day.
 
   Value in incremental should be SQL which is compatible with MYSQL.  
 
 ```json
 [
   {
     "mysql": {...},
     "tables": [
       {
         "name": "test",
         "incremental": "modified_at BETWEEN '2016-12-30 00:00:00' AND '2016-12-30 23:59:59'"
       }
     ],
     "redshift": {...},
     "s3": {...}
   }
 ]
 ```
 
 **Note[2]**- Program will works better if you have only one integer primary key in MySQL Table otherwise you have to 
 provide `distkey` which should be integer and unique. This `distkey` helps program to split data and process it in 
 parallel. If your table doesn't have any of these options then add `"isSplittable": false` field to json so it will not
 split table.

 **Note[3]**- It is better to have an **auto-incremental** column in MySQL which should be only _PRIMARY KEY_ because
  it will help application process data in parallel.

3. **Non-Splittable**:- If table is non-splittable then add `"isSplittable": false`. But large table needs to be splitted otherwise it will
became impossible to migrate table in real-time. By default this field is `true` if not passed.

```json
 [
   {
     "mysql": {...},
     "tables": [
       {
         "name": "test",
         "isSplittable": false
       }
     ],
     "redshift": {...},
     "s3": {...}
   }
 ]
 ```

4. **Add Columns**:- If you wanted additional column in redshift table with some processing which can done in SQL 
compatible with Redshift. Suppose you want to extract `year` and `month` from `created_at` column you can do it in 
following way:-
 
 ```json
  [
    {
      "mysql": {...},
      "tables": [
        {
          "name": "test",
          "addColumn": "EXTRACT(year from created_at)::INT AS year, EXTRACT(month from created_at)::INT AS month"
        }
      ],
      "redshift": {...},
      "s3": {...}
    }
  ]
  ```
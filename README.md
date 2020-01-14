# mysql-general-log-extractor
Extracts mysql general logs for long term storage which can be used for audit purposes.


Mysql removes logs to keep exhausting storage and this can be dictated using storage policy of the logs. For an intance with high volume traffic the storage can get exhausted pretty soon. To investigate the any issue that might be caused by data/schema changes, study of the general logs are very helpful. To increase the retention of the logs, the mysql-general-log-extractor will extract logs from the mysql instance and will store it into AWS S3 buckets. An Athena table is built on this bucket and this table can be used to effectively query this data.

### How to configure:

##### 0. As a prerequisite install [Groovy version 2.4.7](http://groovy-lang.org/) or higher.

##### 1. Create following table in mysql. This is used for coordinating subsequent runs of the extractor in case of failure which ensures no data/logs are missed.

```sql
CREATE TABLE registry
(
   registry_key    VARCHAR(128) NOT NULL,
   registry_value  TEXT,
   PRIMARY KEY (registry_key)
)
ENGINE=InnoDB
```
##### 2. Create an S3 bucket with name ``mysql_general_logs``.
##### 3. Create the Athena table named ``mysql_general_logs``. The following query can be used to create the table:

```sql
CREATE EXTERNAL TABLE `gumgum_rds_general_logs`(
  `event_time` string,
  `user_host` string,
  `thread_id` bigint,
  `server_id` int,
  `command_type` string,
  `statement` string)
PARTITIONED BY (
  `dt` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '^'
  MAP KEYS TERMINATED BY 'undefined'
WITH SERDEPROPERTIES (
  'collection.delim'='undefined')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mysql-general-logs/prod/general'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'transient_lastDdlTime'='1530908667')
```
##### 4. Clone this repository to the instance where the log extraction will run and modify the following constant values in com/roy/mysql/StoreMysqlGeneralLogsToS3.groovy with the respective values for your environment.

```java
    static final def AWS_ACCOUNT_ID = 'PUT_YOUR_AWS_ACCOUNT_ID'
    static final def ATHENA_DB_NAME = 'YOUR_ATHENA_DATABASE_NAME
    static final def AWS_REGION = 'us-east-1'
    static final def ATHENA_TABLE_NAME = 'mysql-general-logs'
```
##### 5. Make sure all permissions and credentials are properly setup. The script needs read and write permission to access Mysql, Athena and S3 (bucket).
##### 6. Set up cron for this script with an hourly frequency. The command must be executed from the project directory. Following is the hourly cron expression sending all the application logs to /var/log directory.
```bash
* 0 0/1 ? * * * groovy com/roy/mysql/StoreMysqlGeneralLogsToS3.groovy >> /var/log/mysql/mysql-to-s3-copy.log 2>&1
```

_If the extraction frequency needs to be lowered to minutes in case the data volume is too large, please modify the following snippet of code under script com/roy/mysql/StoreMysqlGeneralLogsToS3.groovy to handle more frequency._

```java
use(groovy.time.TimeCategory) {
            if(!lastRunTime) { // if no last run defined, start extracting from last 1 hour
                lastRunTime = now - 1.hour() // <-- CHANGE to xx.getMinute() for minute frequency
            }
            def duration = now - lastRunTime
            if (duration.days > 0 || duration.hours > 2) {
                LOGGER.info("Falling behind from log generation by {$duration.days} days and {$duration.hours} hours")
                now = lastRunTime + 1.getHour() + 30.getMinute() // <-- CHANGE to xx.getMinute() for minute frequency. Reduce 30.getMinute() to some appropriate value as well for the catching up.
            }
        }
```
##### 7. After the first extraction executes (monitor the logs). Execute the folliong query in Athena to see the general logs:
```sql
SELECT * FROM "<your_database_name"."mysql_general_logs" limit 10;
```
##### And there you have it!!! If the steps are correctly executed, the general logs will be pulled out from Mysql and dumped to S3 and accesible through Athena. Please note that this does not modify any of the logs in the mysql.

#!/usr/bin/env groovy

package com.roy.mysql

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest
import com.amazonaws.services.athena.model.QueryExecutionContext
import com.amazonaws.services.athena.model.QueryExecutionState
import com.amazonaws.services.athena.model.ResultConfiguration
import com.amazonaws.services.athena.model.StartQueryExecutionRequest

import groovy.sql.Sql
import org.apache.commons.dbcp.BasicDataSource

import com.roy.util.Logger

@Grab('org.apache.commons:commons-lang3:3.5')
@Grab('org.apache.httpcomponents:httpclient:4.5.2')
@Grab('mysql:mysql-connector-java:5.1.35')
@Grab('com.amazonaws:aws-java-sdk:1.11.52')
@Grab('commons-dbcp:commons-dbcp:1.4')
@Grab('commons-codec:commons-codec:1.5')
@Grab('org.rauschig:jarchivelib:0.7.1')
@Grab('javax.mail:mail:1.4.1')
@Grab('commons-collections:commons-collections:3.0')
@Grab('com.amazonaws:aws-java-sdk-athena:1.11.360')

import org.apache.commons.lang3.StringUtils

/**
 * This groovy class pulls the MySql general logs for 1 hour since last run and stores it into a
 * given S3 bucket in a ^ seprated file. This file can be queried through Athena. The goal is to
 * allow storage of Mysql general logs for extended periods of time for investigation and auditing
 * purposes.
 *
 * This code is distributed under GNU-GPL3 licence. Please refer to the license file for more details.
 *
 * Note: Logs are stored with respect to UTC time format with the naming convention of
 * <time-from>_to_<Time-to> where time is in (HH-mm-ss) format.
 *
 * @author Anirban Roy (https://github.com/anirban-roy/)
 */

class StoreMysqlGeneralLogsToS3 {
    static final def LOGGER = new Logger(theClass:this.getClass())

    static final def AWS_ACCOUNT_ID = 'PUT_YOUR_AWS_ACCOUNT_ID'
    static final def ATHENA_DB_NAME = 'YOUR_ATHENA_DATABASE_NAME
    static final def AWS_REGION = 'us-east-1'
    static final def ATHENA_TABLE_NAME = 'mysql-general-logs'

    static final def TIME_FORMATER = "YYYY-MM-dd HH:mm:ss"
    static final def TIME_ZONE_UTC = TimeZone.getTimeZone('UTC')
    static final def TIME_FORMAT_FOR_FILE_NAME = "HH-mm-ss"

    // make sure the credentials are setup properly or choose a different credential provider
    static def awsCredentialsProvider = new DefaultAWSCredentialsProviderChain()

    private static def SEPARATOR = '^'
    private static def LINE_BREAK = '\n'
    private static def S3_BUCKET_NAME = 'gumgum-rds-logs' //'mysql-general-logs'

    // << PASS JDBC Connection Url to mysql database Schema >>
    private static GEN_LOG_INSTANCE = new Sql(getConnectionInstance('jdbc connection url', 'db_user_ame', 'db_password'))
    // << Pass JDBC Connection Url to application database Schema where registry table lives >>
    private static APPL_LOG_INSTANCE = new Sql(getConnectionInstance("jdbc connection url", 'db_user_ame', 'db_password'))

    /**
     * Returns a connection needed to connect to the DB. Change this to override connection settings.
     */
    private static getConnectionInstance(def connectionUrl, def userName, def passwd) {
        def dataSource = new BasicDataSource()
        dataSource.with {
            url = connectionUrl
            username = userName
            password = passwd
            driverClassName = 'com.mysql.jdbc.Driver'
            initialSize = 2
            maxActive = 5
            maxIdle = 5
            minIdle = 0
            maxWait = 10000
            validationQuery = "/* ping */ SELECT 1"
            testOnBorrow = true
            testOnReturn = false
            testWhileIdle = false
            timeBetweenEvictionRunsMillis = 60000
            numTestsPerEvictionRun = 3
            minEvictableIdleTimeMillis = 60000
            removeAbandoned = true
            removeAbandonedTimeout = 120
            logAbandoned = true
            poolPreparedStatements = true
            maxOpenPreparedStatements = 100
        }
        return dataSource
    }

    private Date getLastExecutionTime() {
        def sql = "SELECT registry_value FROM registry WHERE registry_key='script.mysql.gen.log.s3.logs.processed.till'"
        def row = APPL_LOG_INSTANCE.firstRow(sql)
        def lastRun = null
        if (row) {
            try {
                def dateStr = row.registry_value
                lastRun = new Date(Long.parseLong(dateStr) * 1000) // since its stored as Epoch
            } catch(Exception ex) {
                // will return null
                LOGGER.warn("Last run time could not established. Will fall back to default window of 1hr.")
            }
        }
        return lastRun
    }

    /**
     * This method retrieves the Mysql logs for a time window (since its last run) and stores it into S3.
     */
    private void pullMysqlLogsSinceLastRunAndStoreToS3 () {
        def lastRunTime = getLastExecutionTime()
        def now = new Date()

        // Determine the run window
        use(groovy.time.TimeCategory) {
            if(!lastRunTime) { // if no last run defined, start extracting from last 1 hour
                lastRunTime = now - 1.getHour()
            }
            def duration = now - lastRunTime
            if (duration.days > 0 || duration.hours > 2) {
                LOGGER.info("Falling behind from log generation by {$duration.days} days and {$duration.hours} hours")
                now = lastRunTime + 1.getHour() + 30.getMinute() // if it lags, this will make sure the script catches up eventually.
            }
        }

        def dir = new File( '/tmp/mysql/')
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                LOGGER.info("Temp buffer directory created at ${dir.getAbsolutePath()}")
            }
        }
        def fileName = "${lastRunTime.format(TIME_FORMAT_FOR_FILE_NAME, TIME_ZONE_UTC)}_to_${now.format(TIME_FORMAT_FOR_FILE_NAME, TIME_ZONE_UTC)}"
        def buffer = new File(dir, "${fileName}.tmp")
        if (buffer.exists()) {
            if (!buffer.delete()) {
                LOGGER.error("Could not delete temp buffer file. Will retry in next run.")
                return
            }
        }
        buffer.createNewFile()
        LOGGER.info("Temp Buffer File ${buffer.getName()} created.")

        def s3Client = new AmazonS3Client(awsCredentialsProvider)
        def partition = now.format("YYYY-MM-dd", TIME_ZONE_UTC)
        def s3Path = "test/general/dt=$partition/${fileName}.log"
        try {
            if(s3Client.doesObjectExist(S3_BUCKET_NAME, s3Path)) { // may happen rarely but should be handled
                LOGGER.warn("Current time slice log file already exists at s3://$s3Path. Not overwriting file and aborting this run.")
                return
            }
        } catch (AmazonS3Exception ex) {
            // do nothing and proceed
        }
        def startTime = lastRunTime.format(TIME_FORMATER, TIME_ZONE_UTC)
        def endTime = now.format(TIME_FORMATER, TIME_ZONE_UTC)
        def query = "select event_time, user_host, thread_id, server_id, command_type, CONVERT(argument USING utf8) as statement from mysql.general_log WHERE event_time >= $startTime and event_time < $endTime"
        LOGGER.info("Pulling Mysql general_logs between UTC '$startTime' and '$endTime' (exclusive).")
        def data
        GEN_LOG_INSTANCE.eachRow(query) { row ->
                    data = new StringBuilder()
                    data.append(row.event_time.format(TIME_FORMATER, TIME_ZONE_UTC))
                        .append(SEPARATOR)
                        .append(row.user_host)
                        .append(SEPARATOR)
                        .append(row.thread_id)
                        .append(SEPARATOR)
                        .append(row.server_id)
                        .append(SEPARATOR)
                        .append(row.command_type)
                        .append(SEPARATOR)
                        .append(row.statement)
                        .append(LINE_BREAK)
            buffer << data.toString()
        }

        if (StringUtils.isNotBlank(data)) {
            try{
                s3Client.putObject(S3_BUCKET_NAME, s3Path, buffer)
                LOGGER.info("Successfully uploaded logs to s3://$s3Path")
                addNewPartitionToAthenaDataCatalog(partition)
            } catch (Exception e) {
                LOGGER.error("Failed to add new to Athena for duration (UTC) $startTime to $endTime due to $e")
            }
        }
        buffer.deleteOnExit() // remove the tmp buffer file
        updateCurrentExecutionTime(now)
    }

    private void addNewPartitionToAthenaDataCatalog(def partition) {
        def athenaClient = AmazonAthenaClientBuilder.standard()
                    .withRegion(AWS_REGION)
                    .withCredentials(awsCredentialsProvider)
                    .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(60000))
                    .build()

        def queryExecutionContext = new QueryExecutionContext().withDatabase(ATHENA_DB_NAME);

        // The result configuration specifies where the results of the query should go in S3 and encryption options
        // Read more on https://docs.aws.amazon.com/athena/latest/ug/querying.html
        def resultConfiguration = new ResultConfiguration()
                                  .withOutputLocation("s3://aws-athena-query-results-" + AWS_ACCOUNT_ID + "-"+AWS_REGION)

        def startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withQueryString("MSCK REPAIR TABLE " + ATHENA_TABLE_NAME)
                .withQueryExecutionContext(queryExecutionContext)
                .withResultConfiguration(resultConfiguration);

        try {
            def startQueryExecutionResult = athenaClient.startQueryExecution(startQueryExecutionRequest)
            def executionId = startQueryExecutionResult.getQueryExecutionId()
            if (StringUtils.isBlank(executionId)) {
                LOGGER.error("Could NOT refresh newly created partition $partition information to Athena data catalog. Response $startQueryExecutionResult")
                return
            }
            waitForAthenaQueryToComplete(athenaClient, executionId, partition)
        } catch (Exception ex) {
            throw new Exception("Unable to add newly created partition $partition to Athena data catalog.", ex)
        }
    }

    private void waitForAthenaQueryToComplete(def client, def queryExecutionId, def partition) throws InterruptedException {
        def getQueryExecutionRequest = new GetQueryExecutionRequest()
                                           .withQueryExecutionId(queryExecutionId);

        boolean queryStillRunning = true
        while (queryStillRunning) {
            def getQueryExecutionResult = client.getQueryExecution(getQueryExecutionRequest)
            def queryState = getQueryExecutionResult.getQueryExecution().getStatus().getState()
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Athena query Failed to run with Error Message: " + getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Athena Query was cancelled.")
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                queryStillRunning = false;
                LOGGER.info("Successfully added new partition(s) $partition to athena data catalog. Query execution Id: $queryExecutionId")
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(1500);
            }
        }
    }

    private void updateCurrentExecutionTime(def date) {
        Long epoch = date.getTime() / 1000
        def sql = "UPDATE registry SET registry_value = $epoch WHERE registry_key='script.mysql.gen.log.s3.logs.processed.till'"
        APPL_LOG_INSTANCE.execute(sql)
    }

    /**
     * Starting point of this script.
     * @param args
     */
    public static void main(String[] args){
        def logStorer = null
        try {
            logStorer = new StoreMysqlGeneralLogsToS3()
            logStorer.pullMysqlLogsSinceLastRunAndStoreToS3()
        } catch(Exception ex) {
            LOGGER.error("Failed script execution with $ex") // implement alerting on this
        }
    }
}
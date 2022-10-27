package ar.edu.itba.pod.client.query2;

import ar.edu.itba.pod.client.utils.Hazelcast;
import ar.edu.itba.pod.client.utils.PerformanceTimer;
import ar.edu.itba.pod.csv.CsvHelper;
import ar.edu.itba.pod.models.Sensor;
import ar.edu.itba.pod.query2.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.KeyValueSource;
import lombok.Cleanup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Client {

    private final static Logger logger = LoggerFactory.getLogger(Client.class);

    private final static String HZ_READINGS_LIST = Optional
            .ofNullable(System.getenv("HZ_READINGS_LIST"))
            .orElse("g3-query2-readings-list");
    private final static String HZ_JOB_TRACKER = Optional
            .ofNullable(System.getenv("HZ_JOB_TRACKER"))
            .orElse("g3-query2");
    private final static String SENSORS_FILE_NAME = Optional
            .ofNullable(System.getenv("SENSORS_FILE_NAME"))
            .orElse("/sensors.csv");
    private final static String READINGS_FILE_NAME = Optional
            .ofNullable(System.getenv("READINGS_FILE_NAME"))
            .orElse("/readings.csv");
    private final static String EXPORT_FILE_NAME = Optional
            .ofNullable(System.getenv("EXPORT_FILE_NAME"))
            .orElse("/query2.csv");
    private final static String TIME_FILE_NAME = Optional
            .ofNullable(System.getenv("EXPORT_FILE_NAME"))
            .orElse("/time2.csv");

    public static void main(String[] args) {
        logger.info("Query 2 client starting...");

        var parser = new CliParser();
        var arguments = parser.parse(args);

        if (arguments.isEmpty()) {
            logger.error("Invalid arguments");
            return;
        }

        var inPath = arguments.get().getInPath();

        try {
            @Cleanup var timer = new PerformanceTimer(arguments.get().getOutPath() + TIME_FILE_NAME, logger);

            timer.startLoadingDataFromFile();
            // read data from file
            var sensors = CsvHelper.parseSensorFile(inPath + SENSORS_FILE_NAME)
                    .stream()
                    .collect(Collectors.toMap(Sensor::getId, sensor -> sensor));
            var readings = CsvHelper.parseReadingsFile(inPath + READINGS_FILE_NAME)
                    .stream().map(reading -> new QueryReading(reading.getYear(), reading.getDayOfTheWeek(), reading.getHourlyCount()))
                    .toList();
            logger.info("Read {} sensors and {} readings", sensors.size(), readings.size());
            timer.endLoadingDataFromFile();

            logger.info("Starting Hazelcast client...");
            var hazelcast = Hazelcast.connect(arguments.get());
            logger.info("Hazelcast client started!");

            timer.startLoadingDataToHazelcast();
            // load reading list to hazelcast
            IList<QueryReading> readingsList = hazelcast.getList(HZ_READINGS_LIST);
            readingsList.clear();
            readingsList.addAll(readings);
            timer.endLoadingDataToHazelcast();

            // start job from reading list
            var dataSource = KeyValueSource.fromList(readingsList);
            var jobTracker = hazelcast.getJobTracker(HZ_JOB_TRACKER);
            var job = jobTracker.newJob(dataSource);

            timer.startMapReduce();

            // do map, reduce, collate and submit.
            var future = job
                    .mapper(new QueryMapper())
                    .reducer(new QueryReducerFactory())
                    .submit(new QueryCollator());

            var result = future.get();

            // write csv file
            CsvHelper.writeFile(
                    arguments.get().getOutPath() + EXPORT_FILE_NAME,
                    "Year;Weekdays_Count;Weekends_Count;Total_Count",
                    result,
                    entry -> String.format("%d; %d; %d; %d", entry.getKey(), entry.getValue().getFirst(), entry.getValue().getSecond(), entry.getValue().getFirst() + entry.getValue().getSecond())
            );

            timer.endMapReduce();
            readingsList.clear();
            

        } catch (IOException e) {
            logger.error("The files 'sensors.csv' and 'readings.csv' are not in the specified inFolder: {}", arguments.get().getInPath());
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Hazelcast execution error {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);
        } finally {
            // Shutdown
            HazelcastClient.shutdownAll();
        }
    }
}

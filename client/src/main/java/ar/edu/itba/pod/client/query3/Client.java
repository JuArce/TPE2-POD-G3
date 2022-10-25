package ar.edu.itba.pod.client.query3;

import ar.edu.itba.pod.client.query3.CliParser;
import ar.edu.itba.pod.client.utils.Hazelcast;
import ar.edu.itba.pod.client.utils.PerformanceTimer;
import ar.edu.itba.pod.csv.CsvHelper;
import ar.edu.itba.pod.models.Sensor;
import ar.edu.itba.pod.query3.QueryCollator;
import ar.edu.itba.pod.query3.QueryCombinerFactory;
import ar.edu.itba.pod.query3.QueryMapper;
import ar.edu.itba.pod.query3.QueryReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.KeyValueSource;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class Client {

    private final static String HZ_READINGS_LIST = Optional
            .ofNullable(System.getenv("HZ_READINGS_LIST"))
            .orElse("g3-query3-readings-list");
    private final static String HZ_JOB_TRACKER = Optional
            .ofNullable(System.getenv("HZ_JOB_TRACKER"))
            .orElse("g3-query3");
    private final static String SENSORS_FILE_NAME = Optional
            .ofNullable(System.getenv("SENSORS_FILE_NAME"))
            .orElse("/sensors.csv");
    private final static String READINGS_FILE_NAME = Optional
            .ofNullable(System.getenv("READINGS_FILE_NAME"))
            .orElse("/readings.csv");
    private final static String EXPORT_FILE_NAME = Optional
            .ofNullable(System.getenv("EXPORT_FILE_NAME"))
            .orElse("/query3.csv");
    private final static String TIME_FILE_NAME = Optional
            .ofNullable(System.getenv("EXPORT_FILE_NAME"))
            .orElse("/time3.csv");


    public static void main(String[] args) {
        log.info("Query 3 client starting...");

        var parser = new CliParser();
        var arguments = parser.parse(args);

        if (arguments.isEmpty()) {
            log.error("Invalid arguments");
            return;
        }

        var inPath = arguments.get().getInPath();

        try {
            @Cleanup var timer = new PerformanceTimer(arguments.get().getOutPath() + TIME_FILE_NAME, log);

            timer.startLoadingDataFromFile();
            var sensors = CsvHelper.parseSensorFile(inPath + SENSORS_FILE_NAME)
                    .stream()
                    .collect(Collectors.toMap(Sensor::getId, t->t));
//            todo: month New Quay;15782;14/11/2019 11:00
            var readings = CsvHelper.parseReadingsFile(inPath + READINGS_FILE_NAME)
                    .stream()
                    .map(t -> Triple.of(t.getSensorId(), t.getHourlyCount(), String.format("%d/%s/%d %d:00", t.getDayOfTheMonth(), t.getMonth(), t.getYear(), t.getHourOfTheDay() )))
                    .toList();

            log.info("Read {} sensors and {} readings", sensors.size(), readings.size());
            timer.endLoadingDataFromFile();

            log.info("Starting Hazelcast client...");
            var hazelcast = Hazelcast.connect(arguments.get());
            log.info("Hazelcast client started!");

            timer.startLoadingDataToHazelcast();

            IList<Triple<Integer,Integer, String>> readingsList = hazelcast.getList(HZ_READINGS_LIST);
            readingsList.clear();
            readingsList.addAll(readings);

            timer.endLoadingDataToHazelcast();

            var dataSource = KeyValueSource.fromList(readingsList);
            var jobTracker = hazelcast.getJobTracker(HZ_JOB_TRACKER);
            var job = jobTracker.newJob(dataSource);

            timer.startMapReduce();

            var future = job
                    .mapper(new QueryMapper(sensors, arguments.get().getDMin()))
                    .combiner(new QueryCombinerFactory())
                    .reducer(new QueryReducerFactory())
                    .submit(new QueryCollator());

            var result = future.get();

            CsvHelper.writeFile(
                    arguments.get().getOutPath() + EXPORT_FILE_NAME,
                    "Sensor;Total_Count",
                    result,
                    entry -> entry.getKey() + ";" + entry.getValue()
            );

            timer.endMapReduce();

        } catch (IOException e) {
            log.error("The files 'sensors.csv' and 'readings.csv' are not in the specified inFolder: {}", arguments.get().getInPath());
        }
        catch (InterruptedException | ExecutionException e) {
            log.error("Hazelcast execution error {}",e.getMessage(),e);
        }
        catch (Exception e){
            log.error("Unexpected error: {}",e.getMessage(),e);
        }
        finally {
            // Shutdown
            HazelcastClient.shutdownAll();
        }
    }
}

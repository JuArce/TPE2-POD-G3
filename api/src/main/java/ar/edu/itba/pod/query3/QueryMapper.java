package ar.edu.itba.pod.query3;

import ar.edu.itba.pod.models.Sensor;
import ar.edu.itba.pod.models.SensorStatus;
import ar.edu.itba.pod.models.Tuple;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Map;

public class QueryMapper implements Mapper<String, Triple<Integer,Integer, String>,String,Tuple<Long, String>> {
    private final Map<Integer, Sensor> sensors;

    private int minPedestrians;

    public QueryMapper(Map<Integer, Sensor> sensors, int minPedestrians) {
        this.sensors = sensors;
        this.minPedestrians = minPedestrians;
    }

    public void map(String s, Triple<Integer,Integer, String> reading, Context<String,Tuple<Long, String>> context) {
        var sensor = sensors.get(reading.getLeft());
        if (sensor.getStatus() == SensorStatus.ACTIVE && reading.getMiddle() >= minPedestrians)
            context.emit(sensor.getName(),new Tuple<>(Long.valueOf(reading.getMiddle()), reading.getRight()));
    }

}

package ar.edu.itba.pod.query1;

import ar.edu.itba.pod.models.Sensor;
import ar.edu.itba.pod.models.SensorStatus;
import ar.edu.itba.pod.models.Tuple;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;

public class TestMapper implements Mapper<String, Tuple<Integer,Integer>,String,Long> {

    private final Map<Integer, Sensor> sensors;

    public TestMapper(Map<Integer, Sensor> sensors) {
        this.sensors = sensors;
    }

    @Override
    public void map(String s, Tuple<Integer,Integer> reading, Context<String, Long> context) {
        var sensor = sensors.get(reading.first());
//        if (sensor.getStatus() == SensorStatus.ACTIVE)
            context.emit(sensor.getName(), Long.valueOf(reading.second()));
    }
}

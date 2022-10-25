package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class QueryReducerFactory implements ReducerFactory<String, Long,Long> {
    @Override
    public Reducer<Long, Long> newReducer(String integer) {
        return new QueryReducer();
    }
    private class QueryReducer extends Reducer<Long, Long> {
            private volatile long max;
            @Override
            public void beginReduce() {
                max = 0;
            }
            @Override
            public synchronized void reduce(Long integer) {
                if(max < integer)
                    max = integer;
            }

            @Override
            public Long finalizeReduce() {
                return max;
            }
        }
}

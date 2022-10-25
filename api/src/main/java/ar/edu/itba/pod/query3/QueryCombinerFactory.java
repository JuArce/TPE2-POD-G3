package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class QueryCombinerFactory implements CombinerFactory<String,Long,Long> {
    @Override
    public Combiner<Long, Long> newCombiner(String integer) {
        return new QueryCombiner();
    }

    private static class QueryCombiner extends Combiner<Long, Long> {
        private long max = 0;

        @Override
        public  void combine(Long integer) {
            if(max<integer)
                max = integer;
        }
        
        @Override
        public void reset() {
            max = 0;
        }
        
        @Override
        public Long finalizeChunk() {
            return max;
        }
    }
}

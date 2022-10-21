package ar.edu.itba.pod.query1;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class TestCombiner implements CombinerFactory<String,Long,Long> {
    

    @Override
    public Combiner<Long, Long> newCombiner(String integer) {
        return new CombinerTest();
    }

    private static class CombinerTest extends Combiner<Long, Long> {
        private long sum = 0;

        @Override
        public  void combine(Long integer) {
            sum += integer;
        }
        
        @Override
        public void reset() {
            sum = 0;
        }
        
        @Override
        public Long finalizeChunk() {
            return sum;
        }
    }
}

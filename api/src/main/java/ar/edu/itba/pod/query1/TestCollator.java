package ar.edu.itba.pod.query1;

import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TestCollator implements Collator<Map.Entry<String,Long>, List<Map.Entry<String, Long>>> {

    @Override
    public List<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> iterable) {
        return StreamSupport.stream(iterable.spliterator(),false)
                .sorted(new TupleComparator())
                .collect(Collectors.toList());
                
    }

    private static class TupleComparator implements java.util.Comparator<Map.Entry<String,Long>> {
        @Override
        public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
            return switch (o1.getValue().compareTo(o2.getValue())) {
                case 0 -> o1.getKey().compareTo(o2.getKey());
                default -> -o1.getValue().compareTo(o2.getValue());
            };
        }
    }
    
}

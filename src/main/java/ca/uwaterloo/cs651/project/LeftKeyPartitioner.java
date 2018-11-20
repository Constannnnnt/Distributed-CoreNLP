package ca.uwaterloo.cs651.project;

import org.apache.spark.Partitioner;
import java.util.HashMap;
import scala.Tuple2;

public class LeftKeyPartitioner extends Partitioner {

    private int numParts;
    private HashMap<String, Integer> map = new HashMap<>();

    public LeftKeyPartitioner(String [] funcs) {
        numParts = funcs.length;
        for (int k=0; k<numParts; k++)
            map.put(funcs[k], k);
    }

    @Override
    public int numPartitions() {
        return numParts;
    }
 
    @Override
    public int getPartition(Object key) {
        return map.get(((Tuple2)key)._1());
    }
}
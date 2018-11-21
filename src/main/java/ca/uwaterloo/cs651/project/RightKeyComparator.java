package ca.uwaterloo.cs651.project;

import java.util.Comparator;
import java.io.Serializable;
import scala.Tuple2;

public class RightKeyComparator implements Comparator<Tuple2<String, Long>>, Serializable {
    @Override
    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
        if (o1._2() < o2._2())
            return -1;
        else if (o1._2() > o2._2())
            return 1;
        else
            return 0;
    }
}
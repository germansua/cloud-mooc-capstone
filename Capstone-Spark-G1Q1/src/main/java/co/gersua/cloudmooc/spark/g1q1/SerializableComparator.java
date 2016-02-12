package co.gersua.cloudmooc.spark.g1q1;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class SerializableComparator<T extends Tuple2<String, Long>> implements Comparator<T>, Serializable {

    @Override
    public int compare(T o1, T o2) {
        return o1._2().compareTo(o2._2()) * -1;
    }
}

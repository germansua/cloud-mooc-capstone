package co.gersua.cloudmooc.spark.g2q1;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class SerializableComparator<T1 extends Comparable, T2 extends Comparable>
        implements Comparator<Tuple2<T1, T2>>, Serializable {

    @Override
    public int compare(Tuple2<T1, T2> o1, Tuple2<T1, T2> o2) {

        int comparison = o1._1().compareTo(o2._1());
        if (comparison == 0) {
            comparison = o1._2().compareTo(o2._2());
        }
        return comparison;
    }
}

package co.gersua.cloudmooc.mapred.g2q3;

public class Pair<X extends Comparable<? super X>, Y extends Comparable<? super Y>>
        implements Comparable<Pair<X, Y>> {

    public final X x;
    public final Y y;

    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public int compareTo(Pair<X, Y> anotherPair) {
        if (anotherPair == null) {
            return 1;
        }

        int xCompare = x.compareTo(anotherPair.x);
        return xCompare != 0 ? xCompare : y.compareTo(anotherPair.y);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if (x != null ? !x.equals(pair.x) : pair.x != null) return false;
        return y != null ? y.equals(pair.y) : pair.y == null;

    }

    @Override
    public int hashCode() {
        int result = x != null ? x.hashCode() : 0;
        result = 31 * result + (y != null ? y.hashCode() : 0);
        return result;
    }
}

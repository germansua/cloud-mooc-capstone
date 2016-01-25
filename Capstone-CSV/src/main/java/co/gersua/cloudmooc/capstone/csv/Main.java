package co.gersua.cloudmooc.capstone.csv;

import java.io.File;

public class Main {

    public static void main(String[] args) {

        if (args.length == 0) {
            return;
        }

        String rootLocation = args[0];
        CSVUtil.pushResults(new File(rootLocation));
    }
}

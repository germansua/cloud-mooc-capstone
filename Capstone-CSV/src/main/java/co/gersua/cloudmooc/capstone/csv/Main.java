package co.gersua.cloudmooc.capstone.csv;

import co.gersua.cloudmooc.capstone.csv.tx.CSVTransformer;
import co.gersua.cloudmooc.capstone.csv.tx.SeparatorEnum;

import java.io.File;

public class Main {

    public static void main(String[] args) {

        if (args.length == 0) {
            return;
        }

        String rootLocation = args[0];
        File rootLocationFile = new File(rootLocation);
//        CSVUtil.pushResults(rootLocationFile);

        // Transformer for Group 1 Question 1
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g1.q1"), ".tx.g1.q1", SeparatorEnum.NEW_LINE, "\"Origin\"", "\"Dest\"");

        // Transformer for Group 1 Question 2
        CSVTransformer.transformFiles(rootLocationFile,
                new File(rootLocation, "tx.g1.q2"), ".tx.g1.q2", SeparatorEnum.TAB, "\"AirlineID\"", "\"ArrDelayMinutes\"");
    }
}

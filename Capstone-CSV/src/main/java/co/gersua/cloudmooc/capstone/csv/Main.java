package co.gersua.cloudmooc.capstone.csv;

import co.gersua.cloudmooc.capstone.csv.tx.CSVTransformer;

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
        CSVTransformer.transformFiles(rootLocationFile, new File(rootLocation, "tx.g1.q1"), ".tx.g1.q1", "\"Origin\"", "\"Dest\"");
    }
}

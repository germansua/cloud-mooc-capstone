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

//        // Transformer for Group 1 Question 1
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g1.q1"), ".tx.g1.q1", SeparatorEnum.NEW_LINE, "\"Origin\"", "\"Dest\"");

        // Transformer for Group 1 Question 1
        CSVTransformer.transformFiles(rootLocationFile,
                new File(rootLocation, "tx.g1.q1_01"), ".tx.g1.q1_01", SeparatorEnum.TAB, "\"Origin\"", "\"Dest\"");

//        // Transformer for Group 1 Question 2
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g1.q2"), ".tx.g1.q2", SeparatorEnum.TAB, "\"AirlineID\"", "\"ArrDelayMinutes\"");
//
//        // Transformer for Group 1 Question 3
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g1.q3"), ".tx.g1.q3", SeparatorEnum.TAB, "\"DayOfWeek\"", "\"ArrDelayMinutes\"");
//
//        // Transformer for Group 2 Question 1
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g2.q1"), ".tx.g2.q1", SeparatorEnum.TAB,
//                "\"Origin\"", "\"UniqueCarrier\"", "\"DepDelayMinutes\"");


        // Transformer for Group 2 Question 1 - 02 - Spark
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g2.q1-02"), ".tx.g2.q1-02", SeparatorEnum.TAB,
//                "\"Origin\"", "\"UniqueCarrier\"", "\"DepDelay\"");

//
//        // Transformer for Group 2 Question 2
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g2.q2"), ".tx.g2.q2", SeparatorEnum.TAB,
//                "\"Origin\"", "\"Dest\"", "\"DepDelayMinutes\"");


        // Transformer for Group 2 Question 2 - 02 - Spark
        CSVTransformer.transformFiles(rootLocationFile,
                new File(rootLocation, "tx.g2.q2-02"), ".tx.g2.q2-02", SeparatorEnum.TAB,
                "\"Origin\"", "\"Dest\"", "\"DepDelay\"");


//
//        // Transformer for Group 2 Question 3
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g2.q3"), ".tx.g2.q3", SeparatorEnum.TAB,
//                "\"Origin\"", "\"Dest\"", "\"UniqueCarrier\"", "\"ArrDelayMinutes\"");
//
//        // For Group 3 Question 1 the data used is the same that Group 1 Question 1
//
//        // Transformer for Group 3 Question 2
//        CSVTransformer.transformFiles(rootLocationFile,
//                new File(rootLocation, "tx.g3.q2"), ".tx.g3.q2", SeparatorEnum.TAB,
//                "\"Origin\"", "\"Dest\"", "\"FlightDate\"", "\"CRSDepTime\"", "\"ArrDelayMinutes\"");
    }
}

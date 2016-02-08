package co.gersua.cloudmooc.dynamodb;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class Main {

    private static final DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));
    private static final String tableName = "G3Q2Results";

    public static void main(String[] args) {
        Table table = dynamoDB.getTable(tableName);

        if (args.length < 1) {
            System.out.println("No arguments were given!");
            return;
        }

        File folder = new File(args[0]);
        if (!folder.isDirectory()) {
            System.out.println("The input is not a folder");
            return;
        }

        int primaryKey = 0;
        File[] files = folder.listFiles();

        for (File file : files) {
            System.out.println("Processing file: " + file.getName());

            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("\\s");

                    if (values.length < 7) {
                        System.out.println("Incorrect line size");
                        return;
                    }

                    String route = values[0];
                    String firstDepartureDate = values[1];
                    int firstDepartureTime = Integer.valueOf(values[2]);
                    String secondDepartureDate = values[4];
                    int secondDepartureTime = Integer.valueOf(values[5]);
                    long totalDelay = Long.valueOf(values[3]) + Long.valueOf(values[6]);

                    Item item = new Item().withPrimaryKey("Id", primaryKey++)
                            .withString("ROUTE", route)
                            .withString("FIRST_DEP_DATE", firstDepartureDate)
                            .withInt("FIRST_DEP_TIME", firstDepartureTime)
                            .withString("SECOND_DEP_DATE", secondDepartureDate)
                            .withInt("SECOND_DEP_TIME", secondDepartureTime)
                            .withLong("TOTAL_DELAY", totalDelay);
                    table.putItem(item);
                }
            } catch (IOException ex) {
                System.out.println("Problem during reading the file: " + ex);
            }
        }
    }
}

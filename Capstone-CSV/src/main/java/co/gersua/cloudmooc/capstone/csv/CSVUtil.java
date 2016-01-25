package co.gersua.cloudmooc.capstone.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class CSVUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtil.class);
    private static final FileFilter DIRECTORIES_FILTER = file -> file.isDirectory();
    private static final FileFilter CSV_FILES_FILTER = file -> file.getName().toLowerCase().endsWith(".csv");

    public static void pushResults(File location) {
        Map<String, Integer> headersCount = headersCount(location);
        if (headersCount.isEmpty()) {
            return;
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(location, "results.txt")))) {
            for (Map.Entry<String, Integer> entry : headersCount.entrySet()) {
                String line = String.format("%s : %s", entry.getKey(), entry.getValue());
                bw.write(line);
                bw.newLine();
            }
        } catch (IOException ex) {
            LOGGER.error("An exception was thrown.", ex);
        }
    }

    public static Map<String, Integer> headersCount(File location) {
        Map<String, Integer> uniqueHeaderMap = new TreeMap<>();

        if (location != null && location.isDirectory()) {

            File[] csvFiles = location.listFiles(CSV_FILES_FILTER);
            for (File csvFile : csvFiles) {
                Set<String> headers = headers(csvFile);

                for (String header : headers) {
                    Integer headerCounter = uniqueHeaderMap.get(header);
                    headerCounter = headerCounter != null ?  ++headerCounter : Integer.valueOf(1);
                    uniqueHeaderMap.put(header, headerCounter);
                }
            }

            File[] innerDirectories = location.listFiles(DIRECTORIES_FILTER);
            for (File innerDirectory : innerDirectories) {

                Map<String, Integer> innerCount = headersCount(innerDirectory);
                for (Map.Entry<String, Integer> innerEntry : innerCount.entrySet()) {

                    String innerHeader = innerEntry.getKey();
                    Integer innerHeaderValue = innerEntry.getValue();

                    Integer headerCounter = uniqueHeaderMap.get(innerHeader);
                    headerCounter = headerCounter != null ?  headerCounter + innerHeaderValue : innerHeaderValue;
                    uniqueHeaderMap.put(innerHeader, headerCounter);
                }
            }
        }

        return uniqueHeaderMap;
    }

    private static Set<String> headers(File csvFile) {

        Set<String> headers = new HashSet<>();

        if (csvFile != null && csvFile.isFile()) {
            try (Scanner scanner = new Scanner(csvFile)) {
                if (scanner.hasNextLine()) {
                    String headerLine = scanner.nextLine();
                    String[] splittedHeaderLine = headerLine.split(",");

                    for (String header : splittedHeaderLine) {
                        headers.add(header);
                    }
                }
            } catch (FileNotFoundException ex) {
                String message = String.format("File %s was not found.", csvFile);
                LOGGER.error(message, ex);
            }
        }

        return headers;
    }
}

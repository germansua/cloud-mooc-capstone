package co.gersua.cloudmooc.capstone.csv.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSVTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVTransformer.class);
    private static final int HEADER_LINE_NUMBER = 0;
    private static final Pattern CSV_PATTERN = Pattern.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?:,|$)");
    private static final FileFilter DIRECTORIES_FILTER = file -> file.isDirectory();
    private static final FileFilter CSV_FILES_FILTER = file -> file.getName().toLowerCase().endsWith(".csv");

    public static void transformFiles(File location, File commonDestination, String suffix, SeparatorEnum separator, String... headers) {

        if (location == null || !location.isDirectory() || commonDestination == null || headers.length == 0) {
            LOGGER.error("Invalid arguments.");
            return;
        }

        commonDestination.mkdir();
        File[] directories = location.listFiles(DIRECTORIES_FILTER);
        File[] csvFiles = location.listFiles(CSV_FILES_FILTER);

        Arrays.asList(directories).parallelStream().forEach(directory ->
                transformFiles(directory, commonDestination, suffix, separator, headers));

        Arrays.asList(csvFiles).parallelStream().forEach(csvFile -> {
            String fileName = csvFile.getName().concat(suffix).concat(".txt");
            transform(csvFile, new File(commonDestination, fileName), separator, headers);
        });
    }

    public static void transform(File src, File dst, SeparatorEnum separator, String... headers) {

        if (src == null || !src.exists() || dst == null || headers.length == 0) {
            LOGGER.error("Invalid arguments src == null {}, !src.exists() {}, dst == null {} headers.length == 0 {}",
                    src == null, !src.exists(), dst == null, headers.length == 0);
            return;
        }

        LOGGER.info("Processing file: {}", src.getAbsolutePath());
        System.out.println(String.format("Processing file: %s", src.getAbsolutePath()));

        try (BufferedReader br = new BufferedReader(new FileReader(src))) {
            int lineCounter = 0;
            List<Integer> indices = new ArrayList<>();

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(dst))) {

                String line;
                while ((line = br.readLine()) != null) {
                    List<String> words = wordSplitter(line);

                    if (HEADER_LINE_NUMBER == lineCounter++) {
                        for (String header : headers) {
                            int headerIndex = words.indexOf(header);
                            if (headerIndex != -1) {
                                indices.add(headerIndex);
                            }
                        }
                    } else {
                        StringBuffer lineBuffer = new StringBuffer();
                        indices.stream().forEach(index -> lineBuffer.append(words.get(index)).append(separator.getValue()));
                        bw.write(lineBuffer.toString());
                        if (!separator.equals(SeparatorEnum.NEW_LINE)) {
                            bw.newLine();
                        }
                    }
                }
            } catch (IOException ex) {
                LOGGER.error("Something went wrong filtering new file.", ex);
            }
        } catch (IOException ex) {
            LOGGER.error("File: {} not found.", src.getAbsolutePath());
        }
    }

    public static List<String> wordSplitter(String line) {
        List<String> words = new ArrayList<>();
        Matcher matcher = CSV_PATTERN.matcher(line);
        while (matcher.find()) {
            String word = matcher.group();
            if (word.endsWith(",")) {
                word = word.substring(0, word.lastIndexOf(","));
            }

            words.add(word);
        }
        return words;
    }
}

package co.gersua.cloudmooc.capstone.csv;

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

    public static void transformFiles(File location, String suffix, String... headers) {

        if (location == null || !location.isDirectory() || headers.length == 0) {
            LOGGER.error("Invalid arguments.");
            return;
        }

        File[] directories = location.listFiles(DIRECTORIES_FILTER);
        File[] csvFiles = location.listFiles(CSV_FILES_FILTER);

        for (File directory : directories) {
            transformFiles(directory, suffix, headers);
        }

        for (File csvFile : csvFiles) {
            String parent = csvFile.getParent();
            String fileName = csvFile.getName().concat(suffix).concat(".txt");

            transform(csvFile, new File(parent, fileName), headers);
        }
    }

    public static void transform(File src, File dst, String... headers) {

        if (src == null || !src.exists() || dst == null || headers.length == 0) {
            LOGGER.error("Invalid arguments src == null {}, !src.exists() {}, dst == null {} headers.length == 0 {}",
                    src == null, !src.exists(), dst == null, headers.length == 0);
            return;
        }

        try (Scanner scanner = new Scanner(src)) {
            int lineCounter = 0;
            Set<Integer> indices = new TreeSet<>();

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(dst))) {

                while (scanner.hasNextLine()) {
                    List<String> words = wordSplitter(scanner.nextLine());

                    if (HEADER_LINE_NUMBER == lineCounter++) {
                        for (String header : headers) {
                            int headerIndex = words.indexOf(header);
                            if (headerIndex != -1) {
                                indices.add(headerIndex);
                            }
                        }
                    } else {
                        StringBuffer lineBuffer = new StringBuffer();
                        indices.stream().forEach(index -> lineBuffer.append(words.get(index)).append("\n"));
                        lineBuffer.deleteCharAt(lineBuffer.length() - 1);
                        bw.write(lineBuffer.toString());
                        bw.newLine();
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

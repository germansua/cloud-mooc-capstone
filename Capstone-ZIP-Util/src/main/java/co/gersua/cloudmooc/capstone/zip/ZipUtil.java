package co.gersua.cloudmooc.capstone.zip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZipUtil.class);

    private static final FileFilter DIRECTORIES_FILTER = file -> file.isDirectory();
    private static final FileFilter ZIP_FILES_FILTER = file -> file.getName().toLowerCase().endsWith(".zip");

    /**
     * Recursive unzips all zip from the folder given
     * @param folder Initial place in the file system where it start looking recursive .zip files
     */
    public static void deepUnzip(String folder) {

        File folderFile = new File(folder);
        if (!folderFile.exists() || !folderFile.isDirectory()) {
            LOGGER.error("The location: {} does not exist or not represent a directory.", folder);
            return;
        }

        List<File> zipFiles = getZipFiles(folderFile);
        zipFiles.parallelStream().forEach(zip -> unzip(zip));
    }

    private static List<File> getZipFiles(File parentDirectory) {
        List<File> zipList = new ArrayList<>();

        if (parentDirectory.exists() && parentDirectory.isDirectory()) {
            zipList.addAll(Arrays.asList(parentDirectory.listFiles(ZIP_FILES_FILTER)));

            File[] directories = parentDirectory.listFiles(DIRECTORIES_FILTER);
            for (File directory : directories) {
                zipList.addAll(getZipFiles(directory));
            }
        }

        return zipList;
    }

    /**
     * Unzips a given zip file
     * @param zipFile File to unzip
     */
    public static void unzip(File zipFile) {
        if (!zipFile.exists()) {
            LOGGER.error("The file: {} does not exist.", zipFile.getAbsolutePath());
            return;
        }

        String zipName = zipFile.getName();
        int dotZipIndex = zipName.toLowerCase().lastIndexOf(".zip");
        String cotainerFolder = zipName.substring(0, dotZipIndex);

        String outputFolder = zipFile.getParent();
        String fileSeparator = System.getProperty("file.separator");
        boolean successful = true;

        try (FileInputStream fis = new FileInputStream(zipFile)) {
            try (ZipInputStream zis = new ZipInputStream(fis)) {

                ZipEntry nextEntry;
                while ((nextEntry = zis.getNextEntry()) != null) {

                    String fileName = String.format(
                            "%s%s%s%s%s", outputFolder, fileSeparator, cotainerFolder,
                            fileSeparator, nextEntry.getName());
                    File file = new File(fileName);
                    new File(file.getParent()).mkdir();

                    if (nextEntry.isDirectory()) {
                        file.mkdir();
                    } else {
                        byte buffer[] = new byte[4096];
                        int bytesRead;

                        try (FileOutputStream fos = new FileOutputStream(file)) {
                            while ((bytesRead = zis.read(buffer)) != -1) {
                                fos.write(buffer, 0, bytesRead);
                            }
                        } catch (IOException ex) {
                            String message = String.format(
                                    "There was a problem uncompressing the file: %s.", nextEntry.getName());
                            LOGGER.error(message, ex);
                            successful = false;
                        }
                    }
                }
            } catch (IOException ex) {
                LOGGER.error("There was a problem trying to create a ZIP.", ex);
                successful = false;
            }
        } catch (IOException ex) {
            LOGGER.error("There was a problem trying to open a FIS.", ex);
            successful = false;
        }

        LOGGER.info("Was successful result?: {}.\tFile processed: {}\t", successful, zipFile.getAbsolutePath());
    }

    /**
     * Unzips a given zip file given its name
     * @param zipFileName Complete path to the file to unzip
     */
    public static void unzip(String zipFileName) {
        File zipFile = new File(zipFileName);
        unzip(zipFile);
    }
}

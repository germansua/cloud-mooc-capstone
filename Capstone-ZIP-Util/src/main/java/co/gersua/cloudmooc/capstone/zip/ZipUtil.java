package co.gersua.cloudmooc.capstone.zip;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipUtil {

    private static final FileFilter DIRECTORIES_FILTER = file -> file.isDirectory();
    private static final FileFilter ZIP_FILES_FILTER = file -> file.getName().toLowerCase().endsWith(".zip");

    /**
     * Recursive unzips all zip from the folder given
     * @param folder Initial place in the file system where it start looking recursive .zip files
     */
    public static void deepUnzip(String folder) {

        File folderFile = new File(folder);
        if (!folderFile.exists() || !folderFile.isDirectory()) {
            // TODO Log here
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
            // TODO Log here
            return;
        }

        String outputFolder = zipFile.getParent();
        String fileSeparator = System.getProperty("file.separator");

        try (FileInputStream fis = new FileInputStream(zipFile)) {
            try (ZipInputStream zis = new ZipInputStream(fis)) {

                ZipEntry nextEntry;
                while ((nextEntry = zis.getNextEntry()) != null) {
                    String fileName = String.format("%s%s%s", outputFolder, fileSeparator, nextEntry.getName());
                    File file = new File(fileName);

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
                            // TODO Log here
                        }
                    }
                }
            } catch (IOException ex) {
                // TODO Log here
            }
        } catch (IOException ex) {
            // TODO Log here
        }
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

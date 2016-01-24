package co.gersua.cloudmooc.capstone.zip;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipUtil {

    public static void unzip(String zipFileName) {

        File zipFile = new File(zipFileName);
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
}

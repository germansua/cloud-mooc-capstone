package co.gersua.cloudmooc.capstone.zip;

public class Main {

    public static void main(String[] args) {

        if (args.length != 0) {
            String zipFile = args[0];
            ZipUtil.deepUnzip(zipFile);
        }
    }
}


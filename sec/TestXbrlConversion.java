import org.apache.calcite.adapter.sec.XbrlToParquetConverter;
import java.io.File;
import java.util.List;

public class TestXbrlConversion {
    public static void main(String[] args) throws Exception {
        XbrlToParquetConverter converter = new XbrlToParquetConverter();
        
        File xbrlFile = new File("/Volumes/T9/sec-data/dji-real-test/sec-raw/0000320193/000032019323000006/aapl-20221231_htm.xml");
        File outputDir = new File("/tmp/xbrl-test-output");
        outputDir.mkdirs();
        
        System.out.println("Converting XBRL file: " + xbrlFile.getName());
        System.out.println("Input file exists: " + xbrlFile.exists());
        System.out.println("File size: " + xbrlFile.length() + " bytes");
        
        List<File> outputFiles = converter.convert(xbrlFile, outputDir, null);
        
        System.out.println("Conversion completed!");
        System.out.println("Output files created: " + outputFiles.size());
        for (File f : outputFiles) {
            System.out.println("  - " + f.getAbsolutePath() + " (" + f.length() + " bytes)");
        }
    }
}
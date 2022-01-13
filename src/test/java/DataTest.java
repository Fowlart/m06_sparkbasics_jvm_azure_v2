import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;

class DataTest {

    public File[] getFiles() {
        File f = new File("results/final_enriched_data");
        return f.listFiles((dir, name) -> name.endsWith("csv"));
    }

    @Test
    public void checkFullyFilledRowsCount() throws IOException {

        FileInputStream inputStream = null;
        Scanner sc = null;
        int filledRowCounter = 0;

        try {
            File file = getFiles()[0];

            // Check if data in place
            Assertions.assertTrue(file.exists(), "Test file does not exist!");

            inputStream = new FileInputStream(file);
            sc = new Scanner(inputStream, "UTF-8");
            Pattern emptyLineMatcher = Pattern.compile("\"\"");
            while (sc.hasNextLine()) {
                // find rows without empty values
                if (!emptyLineMatcher.matcher(sc.nextLine()).find()) {
                    filledRowCounter++;
                }
                if (sc.ioException() != null) {
                    throw sc.ioException();
                }
            }
            // Test if joined result table has n rows without empty values minimum
            int minimum_meaningful_row_count = 50;
            Assertions.assertTrue(filledRowCounter >= minimum_meaningful_row_count,
                    "Insufficient minimal meaningful row count in the result set!");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }
    }
}
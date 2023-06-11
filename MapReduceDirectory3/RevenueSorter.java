import java.io.*;
import java.util.*;

public class RevenueSorter {
    public static void main(String[] args) {
        try (BufferedReader br = new BufferedReader(new FileReader("C:\\DBMS\\Project\\MapReduceDirectory3\\part-00000.txt"));
             BufferedWriter bw = new BufferedWriter(new FileWriter("output.txt"))) {

            List<String> lines = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }

            // Sort the lines based on the second column (revenue) in decreasing order
            Collections.sort(lines, new RevenueComparator());

            // Write the sorted lines to the output file
            for (String sortedLine : lines) {
                bw.write(sortedLine);
                bw.newLine();
            }
            
            System.out.println("File sorted successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class RevenueComparator implements Comparator<String> {
        @Override
        public int compare(String line1, String line2) {
            String[] parts1 = line1.split(" ");
            String[] parts2 = line2.split(" ");

            double revenue1 = Double.parseDouble(parts1[1]);
            double revenue2 = Double.parseDouble(parts2[1]);

            // Sort in decreasing order
            return Double.compare(revenue2, revenue1);
        }
    }
}

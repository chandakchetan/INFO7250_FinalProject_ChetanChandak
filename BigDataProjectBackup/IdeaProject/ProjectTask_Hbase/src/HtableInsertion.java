import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by chetanchandak on 8/9/15.
 */
public class HtableInsertion {

    public static void main(String[] args) throws IOException {

        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();
        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(config);

        insertRatings(admin, config);
        insertMovieDetails(admin, config);
    }

    public static void insertRatings(HBaseAdmin admin, Configuration config) throws IOException {

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("MovieRatings"));
        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("userId"));
        tableDescriptor.addFamily(new HColumnDescriptor("movieId"));
        tableDescriptor.addFamily(new HColumnDescriptor("rating"));
        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");
        // Instantiating HTable class
        HTable hTable = new HTable(config, "MovieRatings");

        String filename = "/home/chetanchandak/Documents/ml-latest/ratings.csv";
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(filename);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            List<String> separatedTokens = null;
            int count = 0;

            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                separatedTokens = new ArrayList<>();
                // Parse the file to get the tokens
                for (String token : line.split(",")) {
                    separatedTokens.add(token);
                }
                count++;

                // Instantiating Put class
                // accepts a row name.
                String rowID = "row" + count;
                Put p = new Put(Bytes.toBytes(rowID));

                // adding values using add() method
                // accepts column family name, qualifier/row name ,value
                p.add(Bytes.toBytes("userId"),
                        Bytes.toBytes("uID"), Bytes.toBytes(separatedTokens.get(0)));

                p.add(Bytes.toBytes("movieId"),
                        Bytes.toBytes("mID"), Bytes.toBytes(separatedTokens.get(1)));

                p.add(Bytes.toBytes("rating"),
                        Bytes.toBytes("mRating"), Bytes.toBytes(separatedTokens.get(2)));

                // Saving the put Instance to the HTable.
                hTable.put(p);
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file: " + filename);
        }
        catch(IOException ex) {
            System.out.println("Error reading file: " + filename);
        }

        // Instantiating Get class
        Get g = new Get(Bytes.toBytes("row1"));

        // Reading the data
        Result result = hTable.get(g);

        // Reading values from Result class object
        byte [] value = result.getValue(Bytes.toBytes("userId"),Bytes.toBytes("uID"));
        byte[] value1 = result.getValue(Bytes.toBytes("movieId"),Bytes.toBytes("mID"));
        byte[] value2 = result.getValue(Bytes.toBytes("rating"),Bytes.toBytes("mRating"));

        // Printing the values
        String userID = Bytes.toString(value);
        String movieID = Bytes.toString(value1);
        String rating = Bytes.toString(value2);

        System.out.println("For Ratings File....");
        System.out.println("userID: " + userID + ", movieID: " + movieID + ", rating: " +rating);
        // closing HTable
        hTable.close();
    }

    public static void insertMovieDetails(HBaseAdmin admin, Configuration config) throws IOException {

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("MovieDetails"));
        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("movieId"));
        tableDescriptor.addFamily(new HColumnDescriptor("movieTitle"));
        tableDescriptor.addFamily(new HColumnDescriptor("genre"));
        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");
        // Instantiating HTable class
        HTable hTable = new HTable(config, "MovieDetails");

        String filename = "/home/chetanchandak/Documents/ml-latest/movies.csv";
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(filename);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            List<String> separatedTokens = null;
            int count = 0;

            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                separatedTokens = new ArrayList<>();
                // Parse the file to get the tokens
                for (String token : line.split(",")) {
                    separatedTokens.add(token);
                }
                count++;

                // Instantiating Put class
                // accepts a row name.
                String rowID = "row" + count;
                Put p = new Put(Bytes.toBytes(rowID));

                // adding values using add() method
                // accepts column family name, qualifier/row name ,value
                p.add(Bytes.toBytes("movieId"),
                        Bytes.toBytes("mID"), Bytes.toBytes(separatedTokens.get(0)));

                p.add(Bytes.toBytes("movieTitle"),
                        Bytes.toBytes("mTitle"), Bytes.toBytes(separatedTokens.get(1)));

                p.add(Bytes.toBytes("genre"),
                        Bytes.toBytes("mGenre"), Bytes.toBytes(separatedTokens.get(2)));

                // Saving the put Instance to the HTable.
                hTable.put(p);
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file: " + filename);
        }
        catch(IOException ex) {
            System.out.println("Error reading file: " + filename);
        }

        // Instantiating Get class
        Get g = new Get(Bytes.toBytes("row1"));

        // Reading the data
        Result result = hTable.get(g);

        // Reading values from Result class object
        byte [] value = result.getValue(Bytes.toBytes("movieId"),Bytes.toBytes("mID"));
        byte[] value1 = result.getValue(Bytes.toBytes("movieTitle"),Bytes.toBytes("mTitle"));
        byte[] value2 = result.getValue(Bytes.toBytes("genre"),Bytes.toBytes("mGenre"));

        // Printing the values
        String movieID = Bytes.toString(value);
        String movieTitle = Bytes.toString(value1);
        String movieGenre = Bytes.toString(value2);

        System.out.println("For Movies File....");
        System.out.println("movieId: " + movieID + ", movieTitle: " + movieTitle + ", genre: " +movieGenre);
        // closing HTable
        hTable.close();
    }

}

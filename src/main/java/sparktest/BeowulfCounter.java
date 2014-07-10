package sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

/**
 * @author kevin
 * @date 7/7/14
 */
public class BeowulfCounter {
    public static void main(String[] args) {
        JavaSparkContext spark = new JavaSparkContext(new SparkConf().setAppName("SparkTest2"));

        // Each element is one line of Beowulf
        JavaRDD<String> beowulfLines = spark.textFile("res/beowulf.txt");

        // Each element is one word of Beowulf
        JavaRDD<String> beowulfWords = beowulfLines
                .flatMap(
                    line -> Arrays.asList(
                        line.replaceAll("[^0-9A-Za-z\\s\\n\\r]", "") // Remove all characters that are not alphanumeric or whitespace, they are probably special markup in the book
                        .toLowerCase() // Remove issues of case-sensitivity
                        .split("[\\s\\n\\r]+")
                    )
                ); // Split each line up word-by-word

        // Creates a mapping of each word that appears to a
        JavaPairRDD<String, Integer> countsOfEachWord =
                beowulfWords
                        .mapToPair(word -> new Tuple2<>(word, 1)) // Each individual word from the beowulfWords object becomes a key-value pair, with a value of 1
                        .reduceByKey((x, y) -> x + y) // All of the values (each of which is set to 1) get added up to give us the number of times this word appears
        ;


        // Sort it by value
        Map<String, Integer> sortedCountsOfEachWord = sortCollection(countsOfEachWord, false);

        // Take the sorted Map and create a String from it
        StringBuilder output = new StringBuilder();
        for (Map.Entry entry : sortedCountsOfEachWord.entrySet()) {
            output.append(entry.getKey()).append("\t\t\t").append(entry.getValue()).append("\n");
        }

        // Write that string to a file
        try {
            PrintWriter outFile = new PrintWriter("res/beowulfCount.txt");
            outFile.write(output.toString());
            outFile.close();
        } catch (FileNotFoundException e) {
            System.err.println("Could not find file");
        }

    }

    public static Map sortCollection(JavaPairRDD countsOfEachWord, boolean byKey) {
        if (byKey) {
            // SORT BY KEY
            return countsOfEachWord.sortByKey().collectAsMap();
        }
        else {
            // SORT BY VALUE
            return sortByValues(countsOfEachWord.collectAsMap(), false);
        }
    }

    /*
    * Java method to sort Map in Java by value e.g. HashMap or Hashtable
    * throw NullPointerException if Map contains null values
    * It also sort values even if they are duplicates
    */
    public static <K extends Comparable,V extends Comparable> Map<K,V> sortByValues(Map<K,V> map, boolean sortAscending){
        List<Map.Entry<K,V>> entries = new LinkedList<>(map.entrySet());

        Collections.sort(entries, (o1, o2) -> (sortAscending? 1 : -1) * o1.getValue().compareTo(o2.getValue())); // Use -1 to sort in the opposite order

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K,V> sortedMap = new LinkedHashMap<>();

        for(Map.Entry<K,V> entry: entries){
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

}

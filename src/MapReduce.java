/**
 * Niall Grogan - 12429338
 * Stephen Dooley - 12502947
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class MapReduce {

    public static void main(String[] args) {

        // get number of files to be reduced from the user
        Scanner reader = new Scanner(System.in);
        System.out.println("Enter the number of files in database:");
        int poolSize = reader.nextInt();

        //PART 1
        // create an input with the name of a file and a String
        // containing every word in the file
        Map<String, String> input = new HashMap<String, String>();
        // get names of the files to search from the program arguments
        for(String fileName:args) {
            String everything = "";
            try {
                // create a buffered reader and read each line of the file
                BufferedReader br = new BufferedReader(new FileReader(fileName));
                // for concatenating multiple strings in a loop
                // much faster than String object in loop concatenation
                StringBuilder sb = new StringBuilder();
                // read a line from the file
                String line = br.readLine();

                // if the line of the file contains text
                while (line != null) {
                    // add the line of the text to the string builder
                    sb.append(line);
                    // add line separator after each line
                    // this ensures words at the beginning and end of new lines
                    // do not concatenate as one
                    sb.append(System.lineSeparator());
                    // read the next line of the file
                    line = br.readLine();
                }
                // convert the string builder to a String containing
                // the entire file
                everything = sb.toString();
                br.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            // add the words to the Hash map
            // contains 3 entries, one for each file
            input.put(fileName,everything);
        }

        {   // APPROACH #3: Distributed MapReduce
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            //Creating a thread pool (of size defined by how many files being map-reduced)
            ExecutorService mapPool3 = Executors.newFixedThreadPool(poolSize);
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();
                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        map(file, contents, mapCallback);
                    }
                });
                //Adding thread to threadpool
                mapPool3.execute(t);
            }
            mapPool3.shutdown();
            // TIME THE MAP PHASE
            long startTime = System.currentTimeMillis();
            //Ensures all threads complete
            while (!mapPool3.isTerminated()) {}
            long mapTime = System.currentTimeMillis() - startTime;

            // GROUP:
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            // REDUCE:

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            //Establishing reduce phase thread pool
            ExecutorService reducePool3 = Executors.newFixedThreadPool(poolSize);
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        reduce(word, list, reduceCallback);
                    }
                });
                //adding thread to reduce pool
                reducePool3.execute(t);
            }
            reducePool3.shutdown();

            // TIME THE REDUCE PHASE
            long startReduceTime = System.currentTimeMillis();
            //Ensures all threads complete
            while (!reducePool3.isTerminated()) {}
            long reduceTime = System.currentTimeMillis() - startReduceTime;

            System.out.println("\nApproach 3:");
            System.out.println("Finished all map threads in " + mapTime + "ms");
            System.out.println("Finished all reduce threads in " + reduceTime + "ms\n");
            System.out.println(output);
            // Printing output to file
            try {
                PrintWriter out = new PrintWriter("outputApproach3.txt");
                out.println(output);
            }
            catch (Exception e) {}
        } // END APPROACH #3


        {   // APPROACH #4: Distributed MapReduce with Concurrent Data Structures
            final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> output = new ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>();

            // MAP:

            final CopyOnWriteArrayList<MappedItem> mappedItems = new CopyOnWriteArrayList<MappedItem>();

            Iterator<ConcurrentHashMap.Entry<String, String>> inputIter = input.entrySet().iterator();
            //Establishing thread pool for map threads
            ExecutorService mapPool4 = Executors.newFixedThreadPool(poolSize);
            while(inputIter.hasNext()) {
                ConcurrentHashMap.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        map(file, contents, mappedItems);
                    }
                });
                //Adding thread to threadpool
                mapPool4.execute(t);
            }
            mapPool4.shutdown();
            // TIME THE MAP PHASE
            long startTime = System.currentTimeMillis();
            //Ensures all threads complete
            while (!mapPool4.isTerminated()) {}
            long mapTime = System.currentTimeMillis() - startTime;

            // GROUP:
            ConcurrentHashMap<String, CopyOnWriteArrayList<String>> groupedItems = new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                CopyOnWriteArrayList<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new CopyOnWriteArrayList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }


            Iterator<ConcurrentHashMap.Entry<String, CopyOnWriteArrayList<String>>> groupedIter = groupedItems.entrySet().iterator();
            //Reduce thread pool creation
            ExecutorService reducePool4 = Executors.newFixedThreadPool(poolSize);
            while(groupedIter.hasNext()) {
                ConcurrentHashMap.Entry<String, CopyOnWriteArrayList<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final CopyOnWriteArrayList<String> list = entry.getValue();

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        reduce(word, list, output);
                    }
                });
                //addign to thread pool
                reducePool4.execute(t);
            }
            reducePool4.shutdown();
            // TIME THE REDUCE PHASE
            long startReduceTime = System.currentTimeMillis();
            //Ensures all threads complete
            while (!reducePool4.isTerminated()) {}
            long reduceTime = System.currentTimeMillis() - startReduceTime;

            System.out.println("\nApproach 4:");
            System.out.println("Finished all map threads in " + mapTime + "ms");
            System.out.println("Finished all reduce threads in " + reduceTime + "ms\n");
            System.out.println(output);
            //Printing output to file
            try {
                PrintWriter out = new PrintWriter("outputApproach4.txt");
                out.println(output);
            }
            catch (Exception e) {}
        } // END APPROACH #4
    }



    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    public static void map(String file, String contents, CopyOnWriteArrayList<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, CopyOnWriteArrayList<String> list, ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> output) {
        ConcurrentHashMap<String, Integer> reducedList = new ConcurrentHashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
}


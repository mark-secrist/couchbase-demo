package com.example;

import java.util.List;


public class Main {
    static String connectionString = "cb.rfxt9efmqrycj5-q.cloud.couchbase.com";
    static String username = "cbuser";
    static String password = "Passw0rd!";
    static String bucketName = "couchmusic2";
    static String key = "00011b74-12be-4e60-abbf-b1c8b9b40bfe";

    public static void main(String[] args) {

        CouchbaseRepository repo = new CouchbaseRepository(connectionString, username, password, bucketName);

        // Connect to the cluster
       boolean connected = repo.connect();

       if (connected) {
           // Get an entry and print it
           String entry = repo.getEntryAsString(key);
           System.out.println( entry);
           //Playlist entry = repo.getEntryAsObject(key);
           //System.out.println( entry.getName());

           List<Playlist> queryResults = repo.findByFirstName("Morgan");
           System.out.println("Found " + queryResults.size() + " items");
           queryResults.forEach(item -> {
               System.out.println(item.getName());
           });

           // Close cluster
           repo.close();

       } else {
           System.out.println("Not connected to the database");
       }

    }

}
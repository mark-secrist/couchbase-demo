package com.example;

import java.time.Duration;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;

public class Main {
    static String connectionString = "cb.rfxt9efmqrycj5-q.cloud.couchbase.com";
    static String username = "cbuser";
    static String password = "Passw0rd!";
    static String bucketName = "couchmusic2";
    static String key = "00011b74-12be-4e60-abbf-b1c8b9b40bfe";

    public static void main(String[] args) {

        // Connect to the cluster
        Cluster cluster = connect();

        // Get an entry and print it
        getEntry(cluster, key);

        doQuery(cluster);

        // Close cluster
        cluster.close();

    }

    public static Cluster connect() {
        ClusterOptions clusterOptions = ClusterOptions.clusterOptions(username, password).environment(env -> {
            // Sets a pre-configured profile called "wan-development" to help avoid latency issues
            // when accessing Capella from a different Wide Area Network
            // or Availability Zone (e.g. your laptop).
            env.applyProfile("wan-development");
        });

        return Cluster.connect(
                "couchbases://" + connectionString,
                clusterOptions
        );
    }

    public static void getEntry(Cluster cluster, String key ) {
        // Get a bucket, scope and collection reference
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        Scope scope = bucket.scope("couchify");
        Collection playlistCollection = scope.collection("playlist");

        // Fetch a playlist document
        try {
            var result = playlistCollection.get(key);
            System.out.println( result.contentAsObject().getString("name"));
        } catch (DocumentNotFoundException ex) {
            System.out.println("Failed to find document for key: " + key);
        }

    }

    public static void doQuery(Cluster cluster) {
        // Get a bucket and scope reference
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        Scope scope = bucket.scope("couchify");

        // Do the query
        String query = "SELECT * from playlist where owner.firstName='Morgan' limit 5 ";
        QueryResult result = scope.query(query);
        System.out.println(result.rowsAsObject());
    }
}
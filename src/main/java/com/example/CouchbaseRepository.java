package com.example;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;

import java.time.Duration;
import java.util.List;

public class CouchbaseRepository {
    private String couchbaseUrl;
    private String dbUser;
    private String dbPassword;
    private String bucketName;

    private Cluster cluster = null;

    public CouchbaseRepository(String url, String username, String password, String bucketName) {
        this.couchbaseUrl = url;
        this.dbUser = username;
        this.dbPassword = password;
        this.bucketName = bucketName;
    }

    public boolean connect() {
        boolean connected = false;
        if (cluster == null) {
            ClusterOptions clusterOptions = ClusterOptions.clusterOptions(dbUser, dbPassword).environment(env -> {
                // Sets a pre-configured profile called "wan-development" to help avoid latency issues
                // when accessing Capella from a different Wide Area Network
                // or Availability Zone (e.g. your laptop).
                env.applyProfile("wan-development");
            });
            try {
                this.cluster = Cluster.connect(
                        "couchbases://" + couchbaseUrl,
                        clusterOptions
                );
                connected = true;

            } catch (CouchbaseException ex) {
                System.out.println("Failed to connect to the database");
            }
        } else {
            connected = true;
        }
        return connected;
    }

    public Cluster getCluster() {
        if (cluster == null)
            connect();
        return cluster;
    }

    public String getEntryAsString( String key ) {
        String entry = null;
        if (cluster != null) {
            // Get a bucket, scope and collection reference
            Bucket bucket = cluster.bucket(bucketName);
            bucket.waitUntilReady(Duration.ofSeconds(10));
            Scope scope = bucket.scope("couchify");
            Collection playlistCollection = scope.collection("playlist");

            // Fetch a playlist document
            try {
                var result = playlistCollection.get(key);
                entry = result.contentAsObject().toString();
                //System.out.println( result.contentAsObject().getString("name"));
            } catch (DocumentNotFoundException ex) {
                System.out.println("Failed to find document for key: " + key);
            }
        }
        return entry;
    }

    public Playlist  getEntryAsObject( String key ) {
        Playlist entry = null;
        if (cluster != null) {
            // Get a bucket, scope and collection reference
            Bucket bucket = cluster.bucket(bucketName);
            bucket.waitUntilReady(Duration.ofSeconds(10));
            Scope scope = bucket.scope("couchify");
            Collection playlistCollection = scope.collection("playlist");

            // Fetch a playlist document
            try {
                var result = playlistCollection.get(key);
                entry = result.contentAs(Playlist.class);
                //System.out.println( result.contentAsObject().getString("name"));
            } catch (DocumentNotFoundException ex) {
                System.out.println("Failed to find document for key: " + key);
            }
        }
        return entry;
    }

    public List<Playlist> findByFirstName(String firstName) {
        //String firstName = "Morgan";
        List<Playlist> results = null;

        // Get a bucket and scope reference
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        Scope scope = bucket.scope("couchify");

        // Do the query
        String query = "SELECT playlist.* from playlist where owner.firstName=$firstName limit 5 ";
        QueryOptions options = QueryOptions.queryOptions().parameters(JsonObject.create().put("firstName",firstName));
        try {
            QueryResult result = scope.query(query, options);
            results = result.rowsAs(Playlist.class);
        } catch (CouchbaseException ex) {
            System.out.println("Error while querying: " + ex);
        }
        return results;
    }

    public boolean close() {
        boolean success = false;
        if (cluster != null) {
            try {
                cluster.close();
            } catch (CouchbaseException ex) {
                System.out.println("Exception closing database: " + ex);
            }
        }

        return success;

    }
}

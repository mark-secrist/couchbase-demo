import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.diagnostics.PingState;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.example.CouchbaseRepository;
import com.example.Playlist;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CouchbaseTests {
    static String connectionString = "cb.rfxt9efmqrycj5-q.cloud.couchbase.com";
    static String username = "cbuser";
    static String password = "Passw0rd!";
    static String bucketName = "couchmusic2";
    static String key = "00011b74-12be-4e60-abbf-b1c8b9b40bfe";

    @Test
    @Order(1)
    public void testConnect() {
        CouchbaseRepository repo = new CouchbaseRepository(connectionString, username, password, bucketName);
        Cluster cluster = repo.getCluster();
        assertNotNull(cluster, "Cluster should not be NULL. Have you configured the Cluster object?");
        cluster.waitUntilReady(Duration.parse("PT10S"));
        PingResult pingResult = cluster.ping();
        assertNotEquals(0, pingResult.endpoints().size(), "Available endpoints is 0, likely due to authentication failure");
        PingState pingState = pingResult.endpoints().get(ServiceType.QUERY).get(0).state();
        assertEquals(PingState.OK, pingState, "Expected state to be 'OK'");
        repo.close();
    }

    @Test
    @Order(2)
    void validateBucket() {
        CouchbaseRepository repo = new CouchbaseRepository(connectionString, username, password, bucketName);
        Cluster cluster = repo.getCluster();
        Bucket bucket = cluster.bucket(bucketName);
        assertNotNull(bucket, "Bucket should not be NULL. Have you configured the Bucket object?");
        bucket.waitUntilReady(Duration.parse("PT10S"));
        PingResult pingResult = bucket.ping();

        assertNotEquals(0, pingResult.endpoints().size(), "Available endpoints is 0, likely due to authentication failure");
        PingState pingState = pingResult.endpoints().get(ServiceType.QUERY).get(0).state();
        assertEquals(PingState.OK, pingState, "Expected state to be 'OK'");
        repo.close();
    }

    @Test
    @Order(3)
    public void validateGetAsString() {
        CouchbaseRepository repo = new CouchbaseRepository(connectionString, username, password, bucketName);
        repo.connect();

        String expected = "{\"owner\":{\"firstName\":\"Morgan\",\"lastName\":\"Moreau\",\"created\":1423827257000,\"title\":\"Mr\",\"updated\":\"2015-08-25T10:27:56\",\"picture\":{\"large\":\"https://randomuser.me/api/portraits/men/34.jpg\",\"thumbnail\":\"https://randomuser.me/api/portraits/thumb/men/34.jpg\",\"medium\":\"https://randomuser.me/api/portraits/med/men/34.jpg\"},\"username\":\"stockadeseffusing18695\"},\"visibility\":\"PUBLIC\",\"created\":\"2014-11-21T23:03:22\",\"name\":\"Playlist # 5 for Morgan\",\"id\":\"00011b74-12be-4e60-abbf-b1c8b9b40bfe\",\"type\":\"playlist\",\"updated\":\"2015-09-11T10:40:01\",\"tracks\":[\"89B8A853A3BDB76276B9F52549EF6099920008DC\",\"535BBDC871157C6873814F69DF1ED1B47A743908\",\"0B358A6A3B31D957A7373D09549B3F8046D112AD\",\"C9FE05D7BA77FF538D8CA2A95E0733AE3248DBFA\",\"61E71BC154D0D57DA0297C50BF270A8783239291\",\"FF6BC306B6FF006B6D6466161B5ADFAFB4457AD5\",\"2438822DD350BD07C982A32D2BAD7341D3CFDDC7\",\"DA7F081047B5452FF2B56F6E28336A54A2363B9B\",\"34D520CF3CEEB131AFF1AFF00FC8E569E1E846C1\",\"18DFA3B55EAC51B98B46B6E5E0B9812C281D2F3A\",\"041EB9B0E8790098922F677A6A629E0B15FDCCCA\",\"DA1D6746DB102E1121DDC3B3FC1FE795462501F9\",\"89B8A853A3BDB76276B9F52549EF6099920008DC\",\"1FDBCABD02D6DC51E0DD058728973759D707370E\",\"ED3334952F4781016C9C5483E87A250B5FF83FE2\"]}";
        String result = repo.getEntryAsString(key);
        assertEquals(expected,result, "The returned result does not match expected");

    }

    @Test
    @Order(4)
    public void validateGetAsObject() {
        CouchbaseRepository repo = new CouchbaseRepository(connectionString, username, password, bucketName);
        repo.connect();
        Playlist result = repo.getEntryAsObject(key);
        assertNotNull(result);
        assertEquals("Morgan", result.getOwner().getFirstName(), "The result does not match expected");

    }

    @Test
    @Order(5)
    public void validateQuery() {
        CouchbaseRepository repo = new CouchbaseRepository(connectionString, username, password, bucketName);
        repo.connect();
        List<Playlist> playlists = repo.doQuery();
        assertNotNull(playlists, "Playlists results should not be null");
        assertEquals(5, playlists.size(), "Expected exactly one playlist to be returned");

    }
}

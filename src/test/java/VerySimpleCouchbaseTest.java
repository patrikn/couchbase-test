import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.transcoder.StringTranscoder;
import static java.lang.String.format;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;
import net.spy.memcached.CASValue;
import net.spy.memcached.transcoders.Transcoder;
import org.junit.Test;
import rx.Notification;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

public class VerySimpleCouchbaseTest
{
    private static final PersistTo PERSIST_21 = PersistTo.ONE;
    private static final net.spy.memcached.PersistTo PERSIST_14 = net.spy.memcached.PersistTo.ONE;
    public static final String UPDATED_JSON = "{\"data\":\"updated json\"";
    public static final String ORIGINAL_JSON = "{\"data\":\"some json\"}";
    private long start;

    @Test
    public void test21() {
        CouchbaseCluster cluster = CouchbaseCluster
                                       .create(DefaultCouchbaseEnvironment.create());
        final AsyncBucket bucket = cluster
                                       .openBucket("cmbucket", "cmpasswd")
                                       .async();

        start = System.nanoTime();
        String version1 = UUID.randomUUID().toString();
        String version2 = UUID.randomUUID().toString();
        String hangerId = UUID.randomUUID().toString();
        RawJsonDocument result =
            bucket.insert(RawJsonDocument.create(version1,
                                                 "{\"data\":\"some json\"}"))
                  .doOnEach(printNotification("create version"))
                  .flatMap(createHanger(bucket, hangerId, version1))
                  .doOnEach(printNotification("create hanger"))
                  .flatMap(update(bucket, version1, version2, hangerId))
                  .toBlocking()
                  .single();
        printMessageWithTimeElapsed("finished: " + result);
    }

    @Test
    public void test14()
        throws Exception
    {
        CouchbaseClient client = new CouchbaseClient(Arrays.asList(new URI("http://127.0.0.1:8091/pools")),
                                                     "cmbucket",
                                                     "cmpasswd");
        start = System.nanoTime();
        String version1 = UUID.randomUUID().toString();
        String version2 = UUID.randomUUID().toString();
        String hangerId = UUID.randomUUID().toString();
        client.set(version1, ORIGINAL_JSON, PERSIST_14);
        printMessageWithTimeElapsed("created version 1");
        client.set(hangerId, hangerWithVersion(version1), PERSIST_14);
        printMessageWithTimeElapsed("created hanger");
        client.set(version2, UPDATED_JSON, PERSIST_14);
        printMessageWithTimeElapsed("created version 2");
        CASValue<Object> data = client.gets(hangerId);
        printMessageWithTimeElapsed("fetched hanger");
        if (((String)data.getValue()).contains(version1)) {
            client.cas(hangerId, data.getCas(), hangerWithVersion(version2), PERSIST_14);
            printMessageWithTimeElapsed("updated hanger");
        }
        else {
            client.delete(version2);
            printMessageWithTimeElapsed("conflict, deleted version 2");
        }
    }

    private Func1<RawJsonDocument, Observable<? extends RawJsonDocument>> createHanger(final AsyncBucket bucket,
                                                                                       final String hangerId,
                                                                                       final String version1)
    {
        return (RawJsonDocument doc)
                     -> bucket.insert(RawJsonDocument.create(hangerId,
                                                             hangerWithVersion(version1)),
                                      PERSIST_21);
    }

    private Func1<RawJsonDocument, Observable<? extends RawJsonDocument>> update(final AsyncBucket bucket,
                                                                                 final String version1,
                                                                                 final String version2,
                                                                                 final String hangerId)
    {
        return (RawJsonDocument doc) -> {
            RawJsonDocument newVersionDoc = RawJsonDocument.create(version2,
                                                                   UPDATED_JSON);
            return Observable.zip(getHanger(bucket, hangerId),
                                  writeVersion(bucket, newVersionDoc),
                                  (RawJsonDocument hanger, Boolean success) -> hanger)
                             .flatMap(updateHangerOrCleanup(bucket, hangerId, version2,
                                                            version1, newVersionDoc));
        };
    }

    private Func1<RawJsonDocument, Observable<? extends RawJsonDocument>> updateHangerOrCleanup(final AsyncBucket bucket,
                                                                                                final String hangerId,
                                                                                                final String newVersion,
                                                                                                final String expectedVersion,
                                                                                                final RawJsonDocument newVersionDoc)
    {
        return (RawJsonDocument hanger) -> {
            long cas = hanger.cas();
            return hanger.content().contains(expectedVersion)
                   ? updateHanger(bucket, newVersion, hangerId, cas)
                   // Conflict, remove version
                   : bucket.remove(newVersionDoc, PersistTo.NONE);
        };
    }

    private Observable<RawJsonDocument> updateHanger(final AsyncBucket bucket,
                                                     final String oldVersion, final String hangerId,
                                                     final long cas)
    {
        return bucket.replace(RawJsonDocument.create(hangerId,
                                                     hangerWithVersion(oldVersion),
                                                     cas),
                              PERSIST_21);
    }

    private Observable<Boolean> writeVersion(final AsyncBucket bucket,
                                             final RawJsonDocument newVersionDoc)
    {
        return bucket.insert(newVersionDoc,
                             PERSIST_21)
              .doOnEach(printNotification("wrote new version"))
              .map((RawJsonDocument newVersion) -> true);
    }

    private Observable<RawJsonDocument> getHanger(final AsyncBucket bucket, final String hangerId)
    {
        return bucket.get(hangerId, RawJsonDocument.class)
              .doOnEach(printNotification("get hanger"));
    }

    private String hangerWithVersion(final String version1)
    {
        return format("{\"pointer\":\"%s\"}",
               version1);
    }

    private <T> Action1<Notification<? super T>> printNotification(final String s)
    {
        return (Notification<? super T> notification) -> {
            String notificationString = notification.toString();
            printMessageWithTimeElapsed(format("%s: %s", s, notificationString));
        };
    }

    private void printMessageWithTimeElapsed(final String message)
    {
        System.out.printf("%.2f: %s\n", elapsed(), message);
    }

    private double elapsed()
    {
        return (System.nanoTime() - start) / 1_000_000.0;
    }
}

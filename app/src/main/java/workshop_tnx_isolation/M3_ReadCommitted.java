package workshop_tnx_isolation;


import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Eager lock releasing. No dirty reads but it's not repeatable.
 */
public class M3_ReadCommitted {
    private final DbConnector connector = new DbConnector();

    private void createSchema() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS User");
                st.execute("DROP TABLE IF EXISTS Leaderboards");
                st.execute("CREATE TABLE User(username VARCHAR(255) PRIMARY KEY, livesInEurope BOOLEAN, points INT)");
                st.execute("CREATE TABLE Leaderboards(leaderboardName varchar(255) PRIMARY KEY, topScorers VARCHAR(255))");

                st.execute("INSERT INTO User VALUES ('Dorin', true, 100)");
                st.execute("INSERT INTO User VALUES ('Porin', true, 5)");
                st.execute("INSERT INTO User VALUES ('Xorin', false, 100)");
                st.execute("INSERT INTO User VALUES ('Borin', false, 100)");

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    private void relocateDorin(CountDownLatch latch) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("START TRANSACTION;");
                st.execute("SELECT SLEEP(3);");
                st.execute("UPDATE User SET livesInEurope = false WHERE username='Dorin';");
                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    /**
     * Generate 2 leaderboards (EU or NON_EU) depending on the user `livesInEurope` or not.
     */
    private void generateLeaderboards(CountDownLatch latch, String isolationLevel) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL " + isolationLevel);
                st.execute("START TRANSACTION;");

                // Create the leader ranking for europe
                List<String> europeTopScorers = new ArrayList<>();
                ResultSet rs = st.executeQuery("SELECT username FROM User WHERE livesInEurope=true AND points >= 100");
                while (rs.next()) {
                    europeTopScorers.add(rs.getString("username"));
                }
                st.execute("INSERT INTO Leaderboards VALUES('EU','" + europeTopScorers + "')");
                st.execute("SELECT SLEEP(6);");

                // Create the leader ranking for non-european countries
                List<String> worldTopScorers = new ArrayList<>();
                ResultSet rs2 = st.executeQuery("SELECT username FROM User WHERE livesInEurope=false AND points >= 100");
                while (rs2.next()) {
                    worldTopScorers.add(rs2.getString("username"));
                }
                st.execute("INSERT INTO Leaderboards VALUES('NON_EU','" + worldTopScorers + "')");

                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    private void printTables() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT * FROM Leaderboards;");
                System.out.println(Util.resultSetToString("Leaderboards", rs, "leaderboardName", "topScorers"));
                rs.close();

                ResultSet rs2 = st.executeQuery("SELECT * FROM User;");
                System.out.println(Util.resultSetToString("User", rs2, "username", "livesInEurope", "points"));
                rs2.close();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    /**
     * In READ COMMITTED isolation level each read statement will lock the row preventing any other transaction from changing it.
     * However, it uses an eager lock releasing scheme. Once a SELECT statement is complete, the locks are released and other transactions
     * can change the row as they please.
     */

    /**
     * What happens here:
     * 1) `generateLeaderboards` transaction does the initial select for EU based users.
     * 2) `moveDorin` transaction waits for the read to finish before it can change it's record.
     * 3) `generateLeaderboards` finishes the first select (which builds the EU leaderboard), `moveDorin` thx has the lock now, it changes the record and commits.
     * 4) `generateLeaderboards` starts the second select, but now it sees "Dorin" is not in europe and adds it NON_EU leaderboard as well.
     **/

    /**
     * This is called a "Non-repeatable read" or "fuzzy read".
     * Under READ COMMITTED isolation mode, 2 reads on the same row from the same transaction can return different values.
     */

    /**
     * Setting the mode to 'REPEATABLE READ' solves this anomaly.
     * A transaction in repeatable read mode will see the state of the DB as of the time when the transaction started.
     * It might get 'stale data' but all the operations will run with the same staleness.
     *
     * This behavior is most similar to how iterators work on a ConcurrentHashMap in java. (and probably other languages as well)
     */


    public static void main(String[] args) throws InterruptedException {
        M3_ReadCommitted sc = new M3_ReadCommitted();
        sc.createSchema();
        sc.printTables();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        exec.execute(() -> sc.relocateDorin(latch));
        exec.execute(() -> sc.generateLeaderboards(latch, "READ COMMITTED"));

        // Running the same transaction with a higher isolation level (REPEATABLE READ) solves our problem.
        // Dorin goes to the EU leaderboard because at the exact point-in-time where `generateLeaderboards` started
        // `relocateDorin` did not commit yet; So `generateLeaderboards` sees an older snapshot of the data where dorin
        // is still in europe. Which is actually correct behavior in most cases. This is why REPEATABLE READ is the default
        // isolation level in MySql.

        //exec.execute(() -> sc.generateLeaderboards(latch,"REPEATABLE READ"));

        latch.await();
        sc.printTables();
        exec.shutdown();
    }

    /**
     * Important clarification on how REPEATABLE_READ is actually implemented by the DBMS:
     * * *
     * The REPEATABLE_READ can be implemented using actual locks. These types of implementations use a 'Two Phase Locking' protocol, (aka 2PL).
     * AFAIK the first generations of databases systems used this approach. But obviously locking rows for the entire duration of a transaction
     * is very bad for concurrency.
     * Use-case : You need to take a database backup. To see a consistent DB state it uses REPEATABLE READ and acquire a lock for each row contained in the DB.
     * Now all your UPDATE statements originating from other transactions are blocked until the backup transaction is finished.
     * From what I understand this is one of the reasons legacy systems have "Nightly server maintenance downtimes".
     * Their legacy DBMS can't take backups concurrently with user traffic.
     * * *
     * Unlike the systems of old DBMS from the Oracle, MySql, PostgreSql family or newer do not actually take locks.
     * They use what is called "Snapshot isolation" implemented using 'Multi Versioning Concurrency Control' scheme aka MVCC.
     * By using MVCC multiple timestamped versions of each row are preserved thus allowing long-running operations (for example analytics or backups)
     * to be executed in parallel with user traffic.
     * The use of MVCC has some fun implications for scenarios involving data mutation like clobbered updates or write skew,
     * but this is outside of scope for this workshop. The hole goes deep.
     */
}

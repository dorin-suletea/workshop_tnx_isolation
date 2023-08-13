package workshop_tnx_isolation;

import com.google.common.base.Strings;

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

    private void printLeaderboardsTable() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT * FROM Leaderboards;");
                System.out.println("Leaderboards============");
                while (rs.next()) {
                    String leaderboardName = Strings.padEnd(rs.getString("leaderboardName"), 6, ' ');
                    String topScorers = rs.getString("topScorers");
                    System.out.println(leaderboardName + " | " + topScorers);
                }
                System.out.println("========================");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    private void printUsersTable() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT * FROM User;");
                System.out.println("Users==================");
                while (rs.next()) {
                    String user = rs.getString("username");
                    boolean livesInEurope = rs.getBoolean("livesInEurope");
                    int points = rs.getInt("points");
                    System.out.println(user + " | " + livesInEurope + " | " + points);
                }
                System.out.println("========================");
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
     *
     * What happens here:
     * `generateLeaderboards` transaction does the initial select for EU based users.
     * `moveDorin` transaction waits for the read to finish before it can change it's record.
     * `generateLeaderboards` finishes the first select, `moveDorin` has the lock now, it changes the record and commits.
     * *
     * `generateLeaderboards` starts the second select, but now it sees "Dorin" was moved out of europe and ads it to the NON_EU leaderboard as well.
     * *
     * This is called a 'Non-repeatable read' or 'fuzzy read'.
     * Under READ COMMITTED isolation mode, 2 subsequent reads on the same row within the same transaction can return different values.
     * The more restrictive "REPEATABLE READ" mode should solve this anomaly. See M4
     */
    public static void main(String[] args) throws InterruptedException {
        M3_ReadCommitted sc = new M3_ReadCommitted();
        sc.createSchema();
        sc.printUsersTable();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        exec.execute(() -> sc.relocateDorin(latch));
        exec.execute(() -> sc.generateLeaderboards(latch,"READ COMMITTED"));

        // Running the same transaction with a higher isolation level solves our problem
        //exec.execute(() -> sc.generateLeaderboards(latch,"REPEATABLE READ"));

        latch.await();
        sc.printUsersTable();
        sc.printLeaderboardsTable();
        exec.shutdown();
    }

}

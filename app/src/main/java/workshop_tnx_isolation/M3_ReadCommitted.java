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
                st.execute("DROP TABLE IF EXISTS UserInventory");
                st.execute("CREATE TABLE UserInventory(username varchar(255) PRIMARY KEY, gbCount int)");
                st.execute("INSERT into UserInventory VALUES ('Dorin', 0);");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    private void dorinPurchases100GoldBars(CountDownLatch latch) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("START TRANSACTION;");
                st.execute("UPDATE UserInventory SET gbCount = 100 WHERE username='Dorin';");
                st.execute("SELECT SLEEP(2);");
                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    /**
     * Let's bet : how much GB dorin has when both 'purchase100GoldBars' and 'giveAwayForUsersWithZeroGoldBars'
     * transactions finish executing. Note that 'purchase100GoldBars' sets the amount to 100, it does not increment by 100.
     * * 10?
     * * 100?
     * * 110?
     */
    private void giveAwayForUsersWithZeroGoldBars(CountDownLatch latch) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
                st.execute("START TRANSACTION;");

                // Collect a list of users with 0 GB in their account
                ResultSet rs = st.executeQuery("SELECT username, gbCount from UserInventory");
                List<String> usersEligibleForGiveaway = new ArrayList<>();
                while (rs.next()) {
                    String userName = rs.getString("username");
                    Integer gbCount = rs.getInt("gbCount");
                    if (gbCount == 0) {
                        usersEligibleForGiveaway.add(userName);
                        System.out.println("Eligible user found: " + userName + " | " + gbCount);
                    }

                }
                st.execute("SELECT SLEEP(6);");

                // Give each one of them 10 GB
                for (String username : usersEligibleForGiveaway) {
                    st.execute("UPDATE UserInventory SET gbCount = gbCount + 10 WHERE username='" + username + "';");
                }
                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    private void printTable() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT * FROM UserInventory;");
                System.out.println("========================");
                while (rs.next()) {
                    String ret = rs.getString("username") + " | " + rs.getInt("gbCount");
                    System.out.println(ret);
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
     * However, it uses an eager lock releasing scheme. Once the read statement is done, the lock is released
     * and any other transaction can change it at will.
     * *
     * What actually happens in this example:
     * TnxA = giveAwayForUsersWithZeroGoldBars
     * TnxB = dorinPurchases100GoldBars
     * *
     * TnxA : Runs the initial select acquiring the locks for the row. TnxB is blocked and waiting for the locks to be available.
     * TnxA : Finishes the initial select and eagerly releases the locks. TnxB goes ahead, sets the value to 100 and commits.
     * TnxA : Based on the first select, does the update (which is effectively: int a = read(a); set(a+10);
     * * * *  But the gbCount is now 100, as it was set by TnxB.
     * *
     * * This is called a 'Non-repeatable read' as a transaction can read the same row twice and receive different values each time.
     */
    public static void main(String[] args) throws InterruptedException {
        M3_ReadCommitted sc = new M3_ReadCommitted();
        sc.createSchema();
        sc.printTable();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        exec.execute(() -> sc.giveAwayForUsersWithZeroGoldBars(latch));
        exec.execute(() -> sc.dorinPurchases100GoldBars(latch));

        latch.await();
        sc.printTable();
        exec.shutdown();
    }

}

package workshop_tnx_isolation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class M2_ReadUncommitted {
    private final DbConnector connector = new DbConnector();

    private void createSchema() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS UserInventory;");
                st.execute("CREATE TABLE UserInventory(username varchar(255) PRIMARY KEY, gbCount int, isRich BOOLEAN)");
                st.execute("INSERT into UserInventory VALUES ('Dorin', 0, false);"); //Dorin is poor, Dorin deserves a raise.
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    private void printTable() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT * FROM UserInventory;");
                System.out.println("========================");
                while (rs.next()) {
                    String ret = rs.getString("username") + " | " + rs.getInt("gbCount") + " | " + rs.getBoolean("isRich");
                    System.out.println(ret);
                }
                System.out.println("========================");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    private void giveDorinMoreMoney(CountDownLatch latch) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("START TRANSACTION;");
                st.execute("UPDATE UserInventory SET gbCount = gbCount + 100 WHERE username='Dorin';");
                st.execute("SELECT SLEEP(5);");
                st.execute("ROLLBACK;"); // LOL
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    /**
     * For connoisseurs :
     * In case you were wandering why I couldn't just run : `UPDATE UserInventory SET isRich = true WHERE gbCount>100`
     * for this example.
     * * *
     * READ UNCOMMITTED makes readers to not request shared locks.
     * Exclusive lock behavior is the same under regardless of the isolation level.
     * * *
     * markRichPeople tnx would have been waiting for the lock held by giveDorinMoreMoney, no race, no dirty read anomaly.
     */
    private void markRichPeople(CountDownLatch latch) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
                st.execute("START TRANSACTION;");
                st.execute("SELECT SLEEP(1);");

                // Which users that have many goldBars?
                ResultSet rs = st.executeQuery("SELECT * FROM UserInventory where gbCount>=100");
                List<String> richPeople = new ArrayList<>();
                while (rs.next()) {
                    richPeople.add(rs.getString("username"));
                }
                rs.close();

                // Mark all of them as rich
                for (String user: richPeople){
                    st.execute("UPDATE UserInventory SET isRich = true WHERE username = '" + user + "'");
                }

                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }


    /**
     * TODO : adjust the  explanation to this example
     -- We see our gbCount==1 but when transaction A rolls back, we see again the original value gbCount==214748370
     -- Until COMMIT or ROLLBACK statements are executed tnx A has no clue if it will run successfully or not.
     -- However we are running our transaction B in "READ UNCOMMITTED" mode
     -- which means "I don't want to wait for any locks, id rather see dirty data".

     -- This is the absolute fastest way to run transactions, but it give you unreliable data back.
     -- When is even this useful?
     -- For read-only analytics where you you have millions of records and some inconsistent data does not change your analysis significantly.
     -- Postgresql (the golden standard for DB correctness) does not even support this mode
     -- https://www.postgresql.org/docs/current/sql-set-transaction.html#:~:text=In%20PostgreSQL%20READ%20UNCOMMITTED%20is,a%20transaction%20has%20been%20executed.

     -- This mode takes no locks, but exhibits "dirty read" anomalies.
     -- Switching to READ COMMITTED mode solves this anomaly.
     */
    public static void main(String[] args) throws InterruptedException {
        M2_ReadUncommitted sc = new M2_ReadUncommitted();
        sc.createSchema();
        sc.printTable();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        exec.execute(() -> sc.giveDorinMoreMoney(latch));
        exec.execute(() -> sc.markRichPeople(latch));

        latch.await();
        sc.printTable();
        exec.shutdown();

    }
}
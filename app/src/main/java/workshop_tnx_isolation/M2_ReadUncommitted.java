package workshop_tnx_isolation;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Can see uncommitted data from other transactions.
 **/
public class M2_ReadUncommitted {
    private final DbConnector connector = new DbConnector();

    private void createSchema() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS MonthlyPay");
                st.execute("DROP TABLE IF EXISTS TaxReport");
                st.execute("CREATE TABLE MonthlyPay(username varchar(255) PRIMARY KEY, paycheck int)");
                st.execute("CREATE TABLE TaxReport(username varchar(255) PRIMARY KEY, isRich boolean)"); //for taxation purposes

                st.execute("INSERT into MonthlyPay VALUES ('RichieRich', 1000)");
                st.execute("INSERT into MonthlyPay VALUES ('RichieNotRich', 20)");
                st.execute("INSERT into MonthlyPay VALUES ('Dorin', 0)"); //Dorin is poor, Dorin deserves a raise.
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
                st.execute("UPDATE MonthlyPay SET paycheck = paycheck + 100 WHERE username='Dorin'");
                st.execute("SELECT SLEEP(8);");
                st.execute("ROLLBACK;"); // LOL
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    /**
     * Insert into tax report everybody that is considered rich (paycheck>=100)
     **/

    /**
     * (For connoisseurs)
     * In case you are wandering why I used 2 tables here :
     * READ UNCOMMITTED makes readers to not request shared locks (reads).
     * However, exclusive locks(writes) behavior is the same regardless of the isolation level.
     * If I were to have a "isRich" column on MonthlyPay and set that to true, this anomaly would not show in the
     * example as 'taxTheRich' transaction would have had to wait for 'giveDorinMoreMoney' transaction to finish
     * before it can update the row, and at that time 'giveDorinMoreMoney' transaction already rolled back.
     */
    private void taxTheRich(CountDownLatch latch, String isolationLevel) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL " + isolationLevel);
                st.execute("START TRANSACTION");
                st.execute("SELECT SLEEP(4)");
                st.execute("INSERT INTO TaxReport SELECT username, paycheck>=100 FROM MonthlyPay");
                st.execute("COMMIT");
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
                ResultSet rs = st.executeQuery("SELECT * FROM MonthlyPay");
                System.out.println(Util.resultSetToString("MonthlyPay", rs, "username", "paycheck"));
                rs.close();

                ResultSet rs2 = st.executeQuery("SELECT * FROM TaxReport");
                System.out.println(Util.resultSetToString("TaxReport", rs2, "username", "isRich"));
                rs2.close();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    /**
     * Starting a transaction in 'READ UNCOMMITTED' mode means it can read data that is currently being dirtied by other transactions.
     * The 'taxTheRich' tnx has no clue if 'giveDorinMoreMoney' will successfully COMMIT or ROLLBACK.
     * But by using 'READ UNCOMMITTED' mode it declares : "I don't want to wait for any locks, id rather see dirty data".
     **/

    /**
     * What happens here:
     * 1) 'giveDorinMoreMoney' starts and sets the field to 100
     * 2) 'taxTheRich' starts, sees the new value (100) and acts on it.
     * 3) 'giveDorinMoreMoney' rolls back, but 'taxTheRich' already acted on the incorrect data.
     **/

    /**
     * This behavior is called "DIRTY READS"
     * and is most similar to the concurrency bugs in classical programming where somebody forgot to use a lock.
     * This is the absolute fastest way to run transactions, but the most incorrect one.
     * Postgresql (the golden standard for DB correctness) does not even support this mode
     * https://www.postgresql.org/docs/current/sql-set-transaction.html#:~:text=In%20PostgreSQL%20READ%20UNCOMMITTED%20is,a%20transaction%20has%20been%20executed.
     */

    /**
     * Using a higher level isolation mode like 'READ COMMITTED' solves this problem.
     * READ COMMITTED does exactly what is sounds it will. The transaction running in this mode can
     * see only mutations that were committed.
     * <p>
     * The exact way of how this is implemented depends on the database system.
     */
    public static void main(String[] args) throws InterruptedException {
        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        M2_ReadUncommitted sc = new M2_ReadUncommitted();
        sc.createSchema();
        sc.printTables();

        exec.execute(() -> sc.giveDorinMoreMoney(latch));
        exec.execute(() -> sc.taxTheRich(latch, "READ UNCOMMITTED"));

        // Running the same transaction in a higher isolation mode solves our problem.
        //exec.execute(() -> sc.taxTheRich(latch, "READ COMMITTED"));

        latch.await();
        sc.printTables();
        exec.shutdown();
    }


}
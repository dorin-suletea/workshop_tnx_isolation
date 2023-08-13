package workshop_tnx_isolation;

import com.google.common.base.Strings;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class M2_ReadUncommitted {
    private final DbConnector connector = new DbConnector();

    private void createSchema() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS MonthlyPay;");
                st.execute("DROP TABLE IF EXISTS TaxReport");
                st.execute("CREATE TABLE MonthlyPay(username varchar(255) PRIMARY KEY, paycheck int)");
                st.execute("CREATE TABLE TaxReport(username varchar(255) PRIMARY KEY, isRich boolean)"); //for taxation purposes

                st.execute("INSERT into MonthlyPay VALUES ('RichieRich', 1000);");
                st.execute("INSERT into MonthlyPay VALUES ('RichieNotRich', 20);");
                st.execute("INSERT into MonthlyPay VALUES ('Dorin', 0);"); //Dorin is poor, Dorin deserves a raise.
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    private void printTable() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                System.out.println("MonthlyPay==============");
                ResultSet rs = st.executeQuery("SELECT * FROM MonthlyPay;");
                while (rs.next()) {
                    String userName = Strings.padEnd(rs.getString("username"), 15, ' ');
                    Integer paycheck = rs.getInt("paycheck");
                    System.out.println(userName + " | " + paycheck);
                }
                rs.close();
                System.out.println("TaxReport===============");
                ResultSet rs2 = st.executeQuery("SELECT * FROM TaxReport;");
                while (rs2.next()) {
                    String userName = Strings.padEnd(rs2.getString("username"), 15, ' ');
                    Boolean isRich = rs.getBoolean("isRich");
                    System.out.println(userName + " | " + isRich);
                }
                rs2.close();
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
                st.execute("UPDATE MonthlyPay SET paycheck = paycheck + 100 WHERE username='Dorin';");
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
     * (For connoisseurs)
     * In case you are wandering why I used 2 tables here :
     * READ UNCOMMITTED makes readers to not request shared locks (reads).
     * However, exclusive locks(writes) behavior is the same regardless of the isolation level.
     * If I were to have a "isRich" column on MonthlyPay and set that to true, this anomaly would not show in the
     * example as 'taxTheRich' transaction would have had to wait for 'giveDorinMoreMoney' transaction to finish
     * before it can update the row, and at that time 'giveDorinMoreMoney' transaction already rolled back.
     */
    private void taxTheRich(CountDownLatch latch) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
                st.execute("START TRANSACTION;");
                st.execute("SELECT SLEEP(4);");
                st.execute("INSERT INTO TaxReport SELECT username, paycheck>=100 FROM MonthlyPay");
                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    /**
     * More on it in M3, but for now we just want to know that it indeed solves our anomaly.
     */
    private void taxTheRichWitReadCommitted(CountDownLatch latch) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
                st.execute("START TRANSACTION;");
                st.execute("SELECT SLEEP(4);");
                st.execute("INSERT INTO TaxReport SELECT username, paycheck>=100 FROM MonthlyPay");
                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    /**
     * a) Starting a transaction in `READ UNCOMMITTED` mode means it can read data that is currently being dirtied by other transactions.
     * *
     * b) When executing our 'taxTheRich' transaction has no clue if 'giveDorinMoreMoney' will successfully COMMIT or ROLLBACK.
     * * READ UNCOMMITTED means : "I don't want to wait for any locks, id rather see dirty data".
     * * This is behavior is somewhat similar to a classical race condition where you see inconsistent values because you forgot to use  syncronized or any sort of mutex.
     * * This anomaly is called "Dirty read"
     * *
     * c) When is this even useful?
     * * It might be an option for read-only analytics on millions of records.
     * * Where some  and some inconsistent data does not change your analysis significantly,
     * * and you are willing to trade correctness for speed.
     * *
     * d) This is the absolute fastest way to run transactions, but the most incorrect one.
     * * Postgresql (the golden standard for DB correctness) does not even support this mode
     * * https://www.postgresql.org/docs/current/sql-set-transaction.html#:~:text=In%20PostgreSQL%20READ%20UNCOMMITTED%20is,a%20transaction%20has%20been%20executed.
     * *
     * e) This mode takes no locks, but exhibits "dirty read" anomalies.
     * * Switching to READ COMMITTED mode solves this anomaly. See M3
     */
    public static void main(String[] args) throws InterruptedException {
        M2_ReadUncommitted sc = new M2_ReadUncommitted();
        sc.createSchema();
        sc.printTable();

        // Run both transactions simultaneously and use the latch to keep the
        // application running until both of the threads finished.
        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        exec.execute(() -> sc.giveDorinMoreMoney(latch));

        exec.execute(() -> sc.taxTheRich(latch));
        // exec.execute(() -> sc.taxTheRichWitReadCommitted(latch));
        // Running the second transaction in READ COMMITTED isolation mode solves our anomaly.

        latch.await();
        sc.printTable();
        exec.shutdown();
    }


}
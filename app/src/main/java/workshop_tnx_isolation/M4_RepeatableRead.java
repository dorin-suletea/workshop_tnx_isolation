package workshop_tnx_isolation;


import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class M4_RepeatableRead {
    private final DbConnector connector = new DbConnector();

    private void createSchema() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS PurchaseCart");
                st.execute("CREATE TABLE PurchaseCart(item VARCHAR(255) PRIMARY KEY, buyer VARCHAR(255) , price INT, taxApplied BOOLEAN)");

                st.execute("INSERT INTO PurchaseCart VALUES ('potatoes', 'Dorin', 10, false)");
                st.execute("INSERT INTO PurchaseCart VALUES ('bread', 'Dorin', 5, false)");
                st.execute("INSERT INTO PurchaseCart VALUES ('12v_Battery', 'Dorin', 40, false)");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    private void taxPayAndShip(CountDownLatch latch, String isolationLevel) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL " + isolationLevel);
                st.execute("START TRANSACTION;");

                // Get items to ship
                List<String> items = new ArrayList<>();
                ResultSet rs = st.executeQuery("SELECT item, price FROM PurchaseCart WHERE buyer='Dorin'");
                while (rs.next()) {
                    items.add(rs.getString("item") + "(" + rs.getInt("price") + ")");
                }
                rs.close();

                // Apply a flat tax on 1 Eur per item
                // https://stackoverflow.com/questions/5444915/how-to-produce-phantom-read-in-repeatable-read-mysql
                st.execute("UPDATE PurchaseCart SET price = price + 1, taxApplied=true WHERE buyer='Dorin';");

                // Calculate totalPrice
                Integer cartPrice = 0;
                ResultSet rs2 = st.executeQuery("SELECT price  FROM PurchaseCart WHERE buyer='Dorin'");
                while (rs2.next()) {
                    cartPrice += rs2.getInt("price");
                }
                rs2.close();

                System.out.println("==================================");
                System.out.println("Shipping " + items + " at price " + cartPrice);
                System.out.println("==================================");
                ResultSet rs3 = st.executeQuery("SELECT * FROM PurchaseCart;");
                System.out.println(Util.resultSetToString("PurchaseCart", rs3, "item", "buyer", "price", "taxApplied"));
                rs3.close();
                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    private void addToCart(CountDownLatch latch) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("START TRANSACTION;");
                st.execute("INSERT INTO PurchaseCart VALUES ('RaspberryPI', 'Dorin', 100, false)");
                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }


    /**
     * REPEATABLE READ guarantees that : "the selects within a transaction will see the same row values even if other transactions changed said values".
     * But it claims nothing about new rows or deleted rows. It makes sense really, how can a transaction acquire a lock on a row that does not yet exist?
     **/

    /**
     * What happens here:
     * 1) `taxPayAndShip` transaction does the initial select to determine which items are supposed to be shipped for "Dorin"
     * 2) `addToCart` concurrently INSERTS a new row for Dorin.
     * Now the total number of rows involved has increased, but since this row was created after the `taxPayAndShip` transaction stated it has no
     * lock or any other concurrency protection.
     * 3) `taxPayAndShip` adds the tax, and sums up the total cost (now including the newly added row) which was not accounted for in the first select.
     * We end up charging the user for an item they won't ever receive.
     **/

    /**
     * This is called "Phantom Read" anomaly. New rows can pop in and out of existence between statements of a transactions.
     */

    /**
     * Setting the isolation level to SERIALIZABLE solves this problem.
     * *
     * The naive way to achieve this isolation level is to put a big lock on the entire table "nothing changes while I run my transaction".
     * Which would yield correct results but will be highly inefficient.
     * *
     * Real systems use a concept called "range locks" instead of a global mutex.
     * While a serializable transaction is active, the DBMS will maintain a set of "currently active where clauses"
     * For every new insertion the system will check if the new record matching any of these active clauses.
     * If yes, the row insertion is throttled, it must wait for the active SERIALIZABLE transaction to end.
     * If not it can go ahead as there is no risk the new row will pop up in any running serializable transactions.
     */

    public static void main(String[] args) throws InterruptedException {
        M4_RepeatableRead sc = new M4_RepeatableRead();
        sc.createSchema();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        exec.execute(() -> sc.addToCart(latch));
        exec.execute(() -> sc.taxPayAndShip(latch, "REPEATABLE READ"));

        // SERIALIZABLE is the strongest isolation level but also the most lock intensive and bad for performance.
        // Serilizability is defined as such : "equivalence of outcome to a serial/sequential schedule".
        // Said more street casually :
        // a) if I run `taxPayAndShip` and `addToCart` sequentially that yields consistent state.
        // b) if I run `addToCart` and `taxPayAndShip` sequentially that also yields consistent state.
        // c) if I run both `taxPayAndShip` and `addToCart` concurrently and I get the same outcome as A or B that's good,
        // there is no concurrency error.

        // If we run the same code in serializable isolation level we get B ordering of mutations which is a valid outcome.
        // Playing with the sleep delays can force the example to give us outcome A, which would also be valid.

        // exec.execute(() -> sc.taxPayAndShip(latch, "SERIALIZABLE"));

        latch.await();
        exec.shutdown();
    }
}

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
                st.execute("CREATE TABLE PurchaseCart(item VARCHAR(255) PRIMARY KEY, buyer VARCHAR(255) , price INT)");

                st.execute("INSERT INTO PurchaseCart VALUES ('potatoes', 'Dorin', 10)");
                st.execute("INSERT INTO PurchaseCart VALUES ('bread', 'Dorin', 5)");
                st.execute("INSERT INTO PurchaseCart VALUES ('12v_Lead-Acid_Battery', 'Dorin', 40)");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return "";
        });
    }

    private void payAndShip(CountDownLatch latch, String isolationLevel) {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL " + isolationLevel);
                st.execute("START TRANSACTION;");

                // Get items to ship
                List<String> items = new ArrayList<>();
                ResultSet rs2 = st.executeQuery("SELECT item, price FROM PurchaseCart WHERE buyer='Dorin'");
                while (rs2.next()) {
                    items.add(rs2.getString("item") + "(" + rs2.getInt("price") + ")");
                }


                Integer cartPrice = 0;
                ResultSet rs = st.executeQuery("SELECT price  FROM PurchaseCart WHERE buyer='Dorin'");
                while (rs.next()) {
                    cartPrice += rs.getInt("price");
                }




                rs2.close();
                System.out.println("===================================");
                System.out.println("Shipping " + items + " at price " + cartPrice);
                System.out.println("===================================");

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
                st.execute("INSERT INTO PurchaseCart VALUES ('RaspberryPI', 'Dorin', 100)");
                st.execute("COMMIT;");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
            return "";
        });
    }

    public static void main(String[] args) throws InterruptedException {
        M4_RepeatableRead sc = new M4_RepeatableRead();
        sc.createSchema();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        exec.execute(() -> sc.addToCart(latch));
        exec.execute(() -> sc.payAndShip(latch, "REPEATABLE READ"));


        latch.await();
        exec.shutdown();
    }
}

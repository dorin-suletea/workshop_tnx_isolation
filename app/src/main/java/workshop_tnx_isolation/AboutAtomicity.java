package workshop_tnx_isolation;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AboutAtomicity {
    private final DbConnector connector = new DbConnector();

    private void createInitialSchema() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS UserInventory;");
                st.execute("CREATE TABLE UserInventory(username varchar(255) PRIMARY KEY, gbCount int)");

                st.execute("INSERT into UserInventory VALUES ('Margot Robbie', 214748370);");
                st.execute("INSERT into UserInventory VALUES ('Julius Caesar', 2147483640);");
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


    // Give each of our users 10 GB each
    private void runGiveawayCampaign() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("UPDATE UserInventory SET gbCount = gbCount + 10 WHERE username='Margot Robbie'");
                st.execute("UPDATE UserInventory SET gbCount = gbCount + 10 WHERE username='Julius Caesar'");

            } catch (Exception e) {
                System.out.println(e);
            }


            return "";
        });
    }


    public static void main(String[] args) throws SQLException {
        AboutAtomicity sc = new AboutAtomicity();
        sc.createInitialSchema();
        sc.printTable();
        System.out.println("Running campaign:");
        sc.runGiveawayCampaign();
        sc.printTable();
    }
}

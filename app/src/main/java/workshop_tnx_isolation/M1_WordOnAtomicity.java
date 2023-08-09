package workshop_tnx_isolation;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class M1_WordOnAtomicity {
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

    /** In case you were interested in experiencing a case of data corruption you are in luck.
     * We lost track of which records were updated and which were not. How do you even recover from that?
     * * a. You re-run the camping and let Margot Robbie have extra 10 gbs.
     * * b. You subtract 10 gbs from everybody and piss off the emperor of roman empire even more.
     * * c. You restore your database from a backup that was done before our campaign ran, losing a bunch of data.
    */
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

    /**
     * Counterintuitively Atomicity as defined by ACID acronym is "All or nothing" guarantee has nothing to do with concurrency.
     * * *
     * Holdup 1: So who controls the concurrency between 2 simultaneous transactions? Isolation, the "I" in acid.
     * Holdup 2: So Dorin is saying while im in a transaction some other transaction might come in and mess with my data? Exactly, see M_2
     */
    private void runGiveawayCampaignAtomically() {
        connector.run(conn -> {
            try (Statement st = conn.createStatement()) {
                st.execute("START TRANSACTION;");
                st.execute("UPDATE UserInventory SET gbCount = gbCount + 10 WHERE username='Margot Robbie';");
                st.execute("UPDATE UserInventory SET gbCount = gbCount + 10 WHERE username='Julius Caesar';");
                st.execute("COMMIT");
            } catch (Exception e) {
                System.out.println(e);
            }
            return "";
        });
    }

    private static void runNonAtomicCampaign(){
        System.out.println("Running camping");
        M1_WordOnAtomicity sc = new M1_WordOnAtomicity();
        sc.createInitialSchema();
        sc.printTable();
        sc.runGiveawayCampaign();
        sc.printTable();
    }

    private static void runAtomicCampaign(){
        System.out.println("Running atomic camping");
        M1_WordOnAtomicity sc = new M1_WordOnAtomicity();
        sc.createInitialSchema();
        sc.printTable();
        sc.runGiveawayCampaignAtomically();
        sc.printTable();
    }



    public static void main(String[] args){
        runNonAtomicCampaign();
        runAtomicCampaign();
    }
}

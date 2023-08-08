package workshop_tnx_isolation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Function;

public class DbConnector {
    private final String url = "jdbc:mysql://localhost:3306/my_playground?useSSL=false";
    private final String username = "usr_dsu";
    private final String password = "password";


    public void run(Function<Connection, String> operation) {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            operation.apply(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

package workshop_tnx_isolation;

import com.google.common.base.Strings;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Util {

    // This formats stuff for printing.
    // Just make sure to keep tableNames under 45ch and columns under 15
    public static String resultSetToString(String tableName, ResultSet rs, String... columns) throws SQLException {
        String ret = new String();
        ret += Strings.padEnd(tableName, 46 - tableName.length(), '=') + "\n";
        for (String colName : columns) {
            ret += Strings.padEnd(colName, 16, ' ') + " | ";
        }
        ret += "\n";
        ret += Strings.padEnd("", 46 - tableName.length(), '-') + "\n";
        while (rs.next()) {
            for (String colName : columns) {
                ret += Strings.padEnd(rs.getObject(colName).toString(), 16, ' ') + " | ";
            }
            ret += "\n";
        }
        return ret;
    }


}

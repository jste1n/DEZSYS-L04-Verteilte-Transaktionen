package node;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * connects to a sqlite db and creates only one table, closing.
 *
 * Created by Jaymes Barker on 03/04/2017.
 */
public class CreateTable {
    private Connection con;

    /**
     * starts the connection
     * creates a db if no is available
     */
    public void connectToDB() {
        // SQLite connection string
        String url = "jdbc:sqlite:kocsis_stein_3.db";
        try {
            con = DriverManager.getConnection(url);
            // set auto-commit mode to false
            con.setAutoCommit(false);
            System.out.println("Opened database successfully123");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    /**
     * creates an empty table
     */
    public void createTable() {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            String sql = "CREATE TABLE SCHUELER " +
                    "(ID INT PRIMARY KEY     NOT NULL," +
                    " NAME           TEXT    NOT NULL, " +
                    " AGE            INT     NOT NULL, " +
                    " CLASS        CHAR(6)) " ;
            stmt.executeUpdate(sql);
            stmt.close();
            con.close();
            System.out.println("Table created successfully");
        } catch ( Exception e ) {
            System.out.println("Table not created");
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        CreateTable db1 = new CreateTable();
        db1.connectToDB();
        db1.createTable();
    }
}

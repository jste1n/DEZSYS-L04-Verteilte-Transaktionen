package node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Scanner;

/**
 * Created by Jaymes Barker on 24/03/2017.
 */
public class Node1 {
    private Connection con;
    private String hostName = "localhost";
    private int portNumber = 4444;
    private int dbServer = 1;

    public Node1() {
        System.out.println("db server nr "+ this.dbServer);
        this.con = null;
    }

    public static void main(String args[]) {
        Node1 n1 = new Node1();
        n1.connectToManager();
    }

    public void connectToManager() {
        try (
                Socket socket = new Socket(hostName, portNumber);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        ) {
            //get from transaction manager
            String fromTM;
            //send to transaction manager
            String toTM = null;
            while (true) {
                System.out.println("waiting for TM request");
                //get from server
                fromTM = in.readLine();
                System.out.println("request from TM: "+fromTM);
                fromTM = fromTM.toLowerCase();


                if (fromTM.equals("prepare")) {
                    toTM = connectToDB();
                    out.println(toTM);
                } else if (fromTM.startsWith("select")) {
                    // do select
                } else if (fromTM.startsWith("update")) {
                    // do update
                } else if (fromTM.startsWith("insert")) {
                    toTM = doInsert(fromTM);
                    out.println(toTM);
                } else if (fromTM.startsWith("delete")) {
                    // do delete
                } else if (fromTM.equals("doabort")) {
                    closeConnectionToDB();
                } else if (fromTM.equals("rollback")) {
                    rollback();
                } else if (fromTM.equals("done")) {
                    closeConnectionToDB();
                }
                //send to server

            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * starts the connection
     * creates a db if no is available
     */
    public String connectToDB() {
        // SQLite connection string
        String url = "jdbc:sqlite:kocsis_stein_"+dbServer+".db";
        String s = "no";
        try {
            con = DriverManager.getConnection(url);
            // set auto-commit mode to false
            con.setAutoCommit(false);
            System.out.println("Opened database successfully");
            s = "yes";
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return s;
    }

    /**
     * closes the current connection to the db
     */
    public void closeConnectionToDB() {
        try {
            con.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * performs a rollback
     * then closes the connection
     */
    public void rollback() {
        try {
            con.rollback();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        closeConnectionToDB();
    }

    /**
     * should be called to execute an insert command
     * @param sql command to execute
     * @return ack or nck
     */
    public String doInsert (String sql) {
        String result = null;
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            sql = "INSERT INTO schueler (ID,NAME,AGE,class) " +
                    "VALUES (8, 'aaa', 19, '5chit' );";
            int rowAffected = stmt.executeUpdate(sql);
            // commit work
            con.commit();

            result = "ack";
            if (rowAffected != 1) {
                result = "nck";
                System.out.println("insert error: no changes");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            result = "nck";
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e3) {
                System.out.println(e3.getMessage());
            }
        }
        System.out.println("Records created successfully");
        return result;
    }
}

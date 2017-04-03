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

    public Node1() {
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
//                Scanner sc = new Scanner(System.in);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        ) {
            //get from transaction manager
            String fromTM;
            //send to transaction manager
            String toTM = null;
            while (true) {
                //get from server
                fromTM = in.readLine();
                fromTM = fromTM.toLowerCase();
                System.out.println(fromTM);

                if (fromTM.equals("prepare")) {
                    toTM = connectToDB();
                } else if (fromTM.startsWith("select")) {
                    // do select
                } else if (fromTM.startsWith("update")) {
                    // do update
                } else if (fromTM.startsWith("insert")) {
                    toTM = doInsert(fromTM);
                } else if (fromTM.startsWith("delete")) {
                    // do delete
                } else if (fromTM.equals("doabort")) {
                    closeConnectionToDB();
                } else if (fromTM.equals("rollback")) {
                    rollback();
                } else if (fromTM.equals("done")) {
                        closeConnectionToDB();
                }

//                toTM = sc.next();
                //send to server
                out.println(toTM);
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String connectToDB() {
        // SQLite connection string
        String url = "jdbc:sqlite:ssNode1.db";
        String s = null;
        try {
            con = DriverManager.getConnection(url);
            // set auto-commit mode to false
            con.setAutoCommit(false);
            System.out.println("Opened database successfully");
            s = "yes";
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            s = "no";
        }
        return s;
    }

    public void closeConnectionToDB() {
        try {
            con.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void rollback() {
        try {
            con.rollback();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        closeConnectionToDB();
    }

    public String doInsert (String sql) {
        String result = null;
        Statement stmt = null;
        try {
            stmt = con.createStatement();
//            String sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) " +
//                    "VALUES (1, 'Paul', 32, 'California', 20000.00 );";

            int rowAffected = stmt.executeUpdate(sql);

            // commit work
            con.commit();

            result = "ack";
            if (rowAffected != 1) {
                result = "nck";
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

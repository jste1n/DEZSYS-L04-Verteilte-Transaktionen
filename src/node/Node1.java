package node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InterfaceAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Scanner;
import java.util.logging.*;

/**
 * Created by Jaymes Barker on 24/03/2017.
 */
public class Node1 {
    private Connection con;
    private String hostName = "localhost";
    private int portNumber = 4444;
    private int dbServer = 2;
    private static final Logger LOGGER = Logger.getLogger(Node1.class.getName());

    public Node1() {
        new Node1(hostName, portNumber);
    }

    public Node1(String arg, int s) {
        this.con = null;
        hostName=arg;
        portNumber=s;
//        System.out.println("host:"+hostName+"|port:"+portNumber+"|");

        //System.out.println("work dir " + System.getProperty("user.dir"));
        Handler fileHandler = null;
        Formatter simpleFormatter = null;

        try {
            // Creating FileHandler
            fileHandler = new FileHandler("./nodeStation"+dbServer+".log", true);
            // Creating SimpleFormatter
            simpleFormatter = new SimpleFormatter();
            // Assigning handler to logger
            LOGGER.addHandler(fileHandler);
            // Setting formatter to the handler
            fileHandler.setFormatter(simpleFormatter);
            // Setting Level to ALL
            LOGGER.setLevel(Level.ALL);
            LOGGER.severe("Node "+dbServer+" started -- " + new SimpleDateFormat("yyyy.MM.dd_HH:mm:ss").format(Calendar.getInstance().getTime()));
        } catch (IOException e) {
            System.err.println("Error is " + e.getMessage());
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Error is " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        Node1 n1 = null;
        if (args.length == 0) {
            n1 = new Node1();
        } else if (args.length == 2) {
            n1 = new Node1(args[0], Integer.parseInt(args[1]));
        } else {
            System.out.println("wrong parameters");
            System.exit(0);
        }
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
            Boolean sendMsg = true;
            while ((fromTM=in.readLine()) != null) {
                //get from server
                //fromTM = in.readLine();
                System.out.println("request from TM: "+fromTM);
                log("Transaction manager: "+fromTM);
                fromTM = fromTM.toLowerCase();


                if (fromTM.equals("prepare")) {
                    toTM = connectToDB();
                    sendMsg=true;
                } else if (fromTM.startsWith("select")) {
                    // do select
                    sendMsg=true;
                } else if (fromTM.startsWith("update")) {
                    // do update
                    sendMsg=true;
                } else if (fromTM.startsWith("insert")) {
                    toTM = doInsert(fromTM);
                    sendMsg=true;
                } else if (fromTM.startsWith("delete")) {
                    // do delete
                    sendMsg=true;
                } else if (fromTM.equals("doabort")) {
                    closeConnectionToDB();
                    sendMsg=false;
                } else if (fromTM.equals("rollback")) {
                    rollback();
                    sendMsg=false;
                } else if (fromTM.equals("done")) {
                    commit();
                    sendMsg=false;
                }

                //send to server
                if (sendMsg) {
                    out.println(toTM);
                    log("Station: "+toTM);
                }
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes the msg in the log file as finest
     *
     * @param s1 msg to write in log file
     */
    //synchronized
    public void log(String s1) {
        LOGGER.finest(s1);
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
            System.out.println("Connection to DB closed");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void commit() {
        // commit work
        try {
            con.commit();
            System.out.println("committed");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        closeConnectionToDB();
    }

    /**
     * performs a rollback
     * then closes the connection
     */
    public void rollback() {
        try {
            con.rollback();
            System.out.println("Did a rollback");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        commit();
    }

    /**
     * should be called to execute an insert command
     * @param sql command to execute
     * @return ack or nck
     */
    public String doInsert (String sql) {
        String result;
        Statement stmt = null;
        try {
            stmt = con.createStatement();
//            sql = "INSERT INTO schueler (ID,NAME,AGE,class) " +
//                    "VALUES (8, 'aaa', 19, '5chit' );";
            int rowAffected = stmt.executeUpdate(sql);

            result = "ack";
            if (rowAffected != 1) {
                result = "nck";
                System.out.println("insert error: no changes");
            } else {
                System.out.println("Records created successfully");
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
        return result;
    }
}

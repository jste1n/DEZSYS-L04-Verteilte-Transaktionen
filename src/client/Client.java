package client;

import node.Node1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

/**
 * client
 * make connection to server
 * will receive msg from others
 * input is outsourced to KeyboardInput class
 * <p>
 * Created by steinwender on 26.11.2016.
 */
public class Client {
    private String hostName = "localhost";
    private int portNumber = 6900;

    /**
     * constructor
     */
    public Client() {
        new Client(hostName, portNumber);
    }

    public Client(String arg, int i) {
        hostName=arg;
        portNumber=i;
        System.out.println("examples: \n" +
                "INSERT INTO schueler (ID,NAME,AGE,class) VALUES (1, 'Kocsis Patrick', 20, '5BHIT' );\n" +
                "INSERT INTO fakenews (ID,NAME) VALUES (1, 'Kocsis Patrick' );");
    }

    /**
     * start socket connection to server
     * get msg sent from other clients
     * start a new thread for user input
     */
    public void connection() {
        try (
                Socket socket = new Socket(hostName, portNumber);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                Scanner sc = new Scanner(System.in);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        ) {

            String fromServer;
            String input = null;
            while (true) {
                input = sc.nextLine();
                //send to server
                out.println(input);

                //get prepare result
                fromServer = in.readLine();
                if(fromServer == null){
                    System.err.println("TransaktionsManager unavailable");
                    System.exit(1);
                }
                System.out.println(fromServer);

                //get prepare station request
                fromServer = in.readLine();
                if(fromServer == null){
                    System.err.println("TransaktionsManager unavailable");
                    System.exit(1);
                }
                System.out.println(fromServer);

                if (!fromServer.contains("doabort")) {
                    // get commit result
                    fromServer = in.readLine();
                    if(fromServer == null){
                        System.err.println("TransaktionsManager unavailable");
                        System.exit(1);
                    }
                    System.out.println(fromServer);
                    // get commit station request
                    fromServer = in.readLine();
                    if(fromServer == null){
                        System.err.println("TransaktionsManager unavailable");
                        System.exit(1);
                    }
                    System.out.println(fromServer);
                }
            }
        } catch (UnknownHostException e) {
            System.err.println("Don't know host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " +
                    hostName);
            System.exit(1);
        }
    }

    /**
     * main method
     * create new object and call connection()
     *
     * @param args hostname(ip) and port number to share
     */
    public static void main(String[] args) {
        Client c1 = null;
        if (args.length == 0) {
            c1 = new Client();
        } else if (args.length == 2) {
            c1 = new Client(args[0], Integer.parseInt(args[1]));
        } else {
            System.out.println("wrong parameters");
            System.exit(0);
        }

        c1.connection();
    }
}

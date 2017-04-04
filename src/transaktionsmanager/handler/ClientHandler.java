package transaktionsmanager.handler;

import protocol.StationProtocol;
import transaktionsmanager.TransaktionsManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by pkocsis on 31.03.17.
 */
public class ClientHandler extends Thread{

    private PrintWriter clientOut;
    private TransaktionsManager transaktionsManager;
    private int port;

    public ClientHandler(TransaktionsManager transaktionsManager,int port){
        this.transaktionsManager=transaktionsManager;
        this.port=port;
    }

    public void notifyClient(String message){
        clientOut.println(message);
    }

    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(this.port)) {
            Socket clientSocket = serverSocket.accept();
            try (
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(
                                    clientSocket.getInputStream()))
            ) {
                String inputLine;
                this.clientOut = out;
                while ((inputLine = in.readLine()) != null) {
                    System.out.println("Message from Client "+inputLine);
                    this.transaktionsManager.startTransaction(inputLine);
                    if(inputLine.equals("beenden")){
                        break;
                    }
                }
                out.flush();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + port);
            System.exit(-1);
        }
    }
}

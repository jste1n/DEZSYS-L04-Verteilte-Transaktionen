package transaktionsmanager.handler;

import transaktionsmanager.TransaktionsManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * Handles connection to Station
 */
public class StationHandler implements Runnable{

    private Socket stationSocket;
    private TransaktionsManager transaktionsManager;
    private int id;
    private PrintWriter out;

    public StationHandler(Socket stationSocket, TransaktionsManager transaktionsManager, int id) {
        this.stationSocket = stationSocket;
        this.transaktionsManager = transaktionsManager;
        this.id=id;
    }

    public void sendRequest(String request){
        out.println(request);
        System.out.println("sent request: "+request);
    }

    public void run() {
        try(
                PrintWriter out = new PrintWriter(stationSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(stationSocket.getInputStream()))
        ) {
            String inputLine;
            this.out=out;
            while(true){
                inputLine = in.readLine();
                System.out.println("station response: "+inputLine);
                this.transaktionsManager.addStationResponse(inputLine, id);
                this.transaktionsManager.updateResponseEvaluation();
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}

package transaktionsmanager.handler;

import transaktionsmanager.TransaktionsManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by pkocsis on 31.03.17.
 */
public class StationHandler extends Thread{

    private Socket stationSocket;
    private TransaktionsManager transaktionsManager;
    private int id;


    public StationHandler(Socket stationSocket, TransaktionsManager transaktionsManager, int id) {
        this.stationSocket = stationSocket;
        this.transaktionsManager = transaktionsManager;
        this.id=id;
    }

    public int getStationId() {
        return id;
    }

    public void run() {
        try(
                PrintWriter out = new PrintWriter(stationSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(stationSocket.getInputStream()))
        ) {
            synchronized(this) {
                String inputLine;
                this.wait();
                String send = this.transaktionsManager.getStationRequest(this.id);
                out.println(send);
                System.out.println("sent to server: " + send);
                while(true) {
                    inputLine = in.readLine();
                    this.transaktionsManager.addStationResponse(inputLine, id);
                    System.out.println("Stationhandler is waiting on request id:"+this.id+" response "+inputLine);
                    this.wait();
                    String response = this.transaktionsManager.getStationRequest(this.id);
                    System.out.println("response "+response);
                    out.println(response);
                }
            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }
}

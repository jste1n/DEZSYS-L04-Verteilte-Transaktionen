package transaktionsmanager;

import protocol.StationProtocol;
import transaktionsmanager.handler.ClientHandler;
import transaktionsmanager.handler.StationHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by pkocsis on 31.03.17.
 */
public class TransaktionsManager {

    private int stationCount = 0;
    private StationProtocol stationProtocol;
    private ClientHandler clientHandler;
    private ArrayList<StationHandler> stationHandlers = new ArrayList<>();
    //private ArrayList<String> stationResponse = new ArrayList<>();
    private HashMap<Integer, String> stationRequest = new HashMap<>();

    private HashMap<Integer, String> stationResponse = new HashMap<>();
    private boolean isStationResponseEvaluated = false;

    public TransaktionsManager() {
        this.clientHandler= new ClientHandler(this);
        this.clientHandler.start();
    }

    public boolean isStationResponseEvaluated() {
        return isStationResponseEvaluated;
    }

    /**
     * this Function gets exectued from the StationHandler after the responses got evaluated
     * @param stationId
     * @return
     */
    public synchronized String getStationRequest(int stationId){
        return this.stationRequest.get(stationId);
    }

    /**
     * Add response from the Station (StationHandler)
     * After all Stations responded the response gets evaluated and a new request gets saved (stationRequest)
     * @param response
     * @param stationHandlerId
     */
    public synchronized void addStationResponse(String response, int stationHandlerId){
        this.stationResponse.put(stationHandlerId,response);
        this.isStationResponseEvaluated=false;
    }

    public synchronized void updateEvaluationStatus(){
        if(stationCount == stationResponse.size()){
            this.stationRequest=this.stationProtocol.processInput(this.stationResponse);
            String clientNotificationText="Station Responses ";
            for(int i =0; i<this.stationResponse.size();i++){
                clientNotificationText += ", "+this.stationResponse.get(i);
            }
            this.clientHandler.notifyClient(clientNotificationText);
            this.stationResponse.clear();
            this.isStationResponseEvaluated = true;
        }
    }

    /**
     * Starts the prepare phase, gets executed from the ClientHandler
     * @param sql
     */
    public void startTransaction(String sql){
        System.out.println("Transaction started");
        for(StationHandler stationHandler:this.stationHandlers){
            this.stationRequest.put(stationHandler.getStationId(),"prepare");
            synchronized(stationHandler){
                stationHandler.notify();
            }
        }
        this.stationProtocol = new StationProtocol(sql);
    }

    /**
     * Listen for Station connections
     * @param port
     */
    public void listenForStations(int port){
        boolean listening = true;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (listening) {
                System.out.println("listening for stations");
                StationHandler tmp_station = new StationHandler(serverSocket.accept(),this,this.stationCount);
                this.stationHandlers.add(this.stationCount,tmp_station);
                tmp_station.start();
                this.stationCount++;
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + port);
            System.exit(-1);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: java TransactionManager <StationListenerPort number>");
            System.exit(1);
        }
        TransaktionsManager transaktionsManager = new TransaktionsManager();
        transaktionsManager.listenForStations(Integer.parseInt(args[0]));
    }
}

package transaktionsmanager;

import protocol.StationProtocol;
import transaktionsmanager.handler.ClientHandler;
import transaktionsmanager.handler.StationHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

/**
 * Created by pkocsis on 31.03.17.
 */
public class TransaktionsManager{

    private int stationCount = 0;
    private int evaluationCounter=0;

    private StationProtocol stationProtocol;
    private ClientHandler clientHandler;
    private static final Logger LOGGER = Logger.getLogger(TransaktionsManager.class.getName());

    private HashMap<Integer,StationHandler> stationHandlers = new HashMap<>();
    private HashMap<Integer, String> stationRequests = new HashMap<>();
    private HashMap<Integer, String> stationResponses = new HashMap<>();

    public TransaktionsManager(int stationPort, int clientPort) {
        Handler fileHandler;
        Formatter simpleFormatter;
        try {
            // Creating FileHandler
            fileHandler = new FileHandler("./TransaktionsManager.log", true);
            // Creating SimpleFormatter
            simpleFormatter = new SimpleFormatter();
            // Assigning handler to logger
            LOGGER.addHandler(fileHandler);
            // Setting formatter to the handler
            fileHandler.setFormatter(simpleFormatter);
            // Setting Level to ALL
            LOGGER.setLevel(Level.ALL);
            LOGGER.severe("TransaktionsManager started -- " + new SimpleDateFormat("yyyy.MM.dd_HH:mm:ss").format(Calendar.getInstance().getTime()));
        } catch (IOException e) {
            System.err.println("Error is " + e.getMessage());
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Error is " + e.getMessage());
            e.printStackTrace();
        }
        this.clientHandler= new ClientHandler(this,clientPort);
        this.clientHandler.start();
        this.listenForStations(stationPort);
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
     * Adds response from the Station (StationHandler)
     * After all Stations responded the response gets evaluated and a new request gets saved (stationRequest)
     * @param response
     * @param stationHandlerId
     */
    public synchronized void addStationResponse(String response, int stationHandlerId){
        this.stationResponses.put(stationHandlerId,response);
        log("Station: #"+stationHandlerId + " msg:" +response);
    }

    /**
     * Gets executed after response from station (in StationHandler) is received
     */
    public void updateResponseEvaluation(){
        evaluationCounter++;
       if(evaluationCounter == stationCount){
           for(Map.Entry<Integer,String> response:this.stationResponses.entrySet()){
               if(response.getValue() == null){
                   this.stationHandlers.remove(response.getKey());
                   this.stationResponses.remove(response.getKey());
               }
           }
           this.stationRequests=this.stationProtocol.processInput(this.stationResponses);
           String clientNotificationText="Station Responses";
           for(Map.Entry<Integer,String> response: this.stationResponses.entrySet()){
               clientNotificationText += " , "+response.getValue();
           }
           this.clientHandler.notifyClient(clientNotificationText);
           clientNotificationText = "TransactionManager request";
           for(Map.Entry<Integer,String> stationRequest:this.stationRequests.entrySet()){
               this.stationHandlers.get(stationRequest.getKey()).sendRequest(stationRequest.getValue());
               log("Transaction manager: #"+stationRequest.getKey() + " request:"+ stationRequest.getValue());
               clientNotificationText += " , "+stationRequest.getValue();
           }
           this.clientHandler.notifyClient(clientNotificationText);
           this.stationResponses.clear();
           this.evaluationCounter=0;
       }
    }

    /**
     * Starts the prepare phase, gets executed from the ClientHandler
     * @param sql
     */
    public void startTransaction(String sql){
        System.out.println("Transaction started");
        for(Map.Entry<Integer,StationHandler> stationHandler:this.stationHandlers.entrySet()){
            stationHandler.getValue().sendRequest("prepare");
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
                this.stationHandlers.put(this.stationCount,tmp_station);
                new Thread(tmp_station).start();
                this.stationCount++;
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + port);
            System.exit(-1);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: java TransactionManager <StationPortNumber> <ClientPortNumber>");
            System.exit(1);
        }
        new TransaktionsManager(Integer.parseInt(args[0]),Integer.parseInt(args[1]));
    }

}

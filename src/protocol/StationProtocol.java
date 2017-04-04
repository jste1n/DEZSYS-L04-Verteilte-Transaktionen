package protocol;

import java.util.ArrayList;

/**
 * Created by pkocsis on 31.03.17.
 */
public class StationProtocol {

    private static final int PREPARE = 0;
    private static final int COMMIT = 1;
    private int state;

    private String sqlCommand;

    public StationProtocol(String sqlCommand) {
        this.sqlCommand = sqlCommand;
        this.state = PREPARE;
    }

    public ArrayList<String> processInput(ArrayList<String> responses) {

        if(state == PREPARE){
            ArrayList<String> requests = new ArrayList<>();
            boolean failed = false;
            for(String response:responses){
                response = response.toLowerCase();
                if(response.equals("no")){
                    failed = true;
                }
            }
            if(!failed){
                for(int i = 0;i<responses.size();i++){
                    requests.add(i,this.sqlCommand);
                }
                this.state=COMMIT;
                return requests;
            }else {
                for(int i = 0;i<responses.size();i++){
                    requests.add(i,"doabort");
                }
                return requests;
            }
        }
        if(this.state == COMMIT){
            ArrayList<String> requests = new ArrayList<>();
            boolean failed = false;
            for(String response:responses){
                if(response.equals("nck")){
                    failed = true;
                }
            }
            if(!failed){
                for(int i = 0;i<responses.size();i++){
                    requests.add(i,"done");
                }
                this.state=COMMIT;
                return requests;
            }else {
                for(int i = 0;i<responses.size();i++){
                    if(responses.get(i).equals("ack")){
                        requests.add(i,"rollback");
                    } else {
                        if(responses.get(i).equals("nck")){
                            requests.add(i,"done");
                        }
                    }
                }
                return requests;
            }
        }
        return null;
    }
}

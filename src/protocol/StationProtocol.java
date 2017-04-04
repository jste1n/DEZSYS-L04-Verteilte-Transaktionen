package protocol;

import java.util.ArrayList;
import java.util.HashMap;

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

    public HashMap<Integer,String> processInput(HashMap<Integer,String> responses) {

        if(state == PREPARE){
            HashMap<Integer,String> requests = new HashMap<>();
            boolean failed = false;
            for(int i = 0;i<responses.size();i++){
                String response = responses.get(i);
                response = response.toLowerCase();
                if(response.equals("no")){
                    failed = true;
                }
            }
            if(!failed){
                for(int i = 0;i<responses.size();i++){
                    requests.put(i,this.sqlCommand);
                }
                this.state=COMMIT;
                return requests;
            }else {
                for(int i = 0;i<responses.size();i++){
                    requests.put(i,"doabort");
                }
                return requests;
            }
        }
        if(this.state == COMMIT){
            HashMap<Integer,String> requests = new HashMap<>();
            boolean failed = false;
            for(int i = 0;i<responses.size();i++){
                String response = responses.get(i);
                response = response.toLowerCase();
                if(response.equals("nck")){
                    failed = true;
                }
            }
            if(!failed){
                for(int i = 0;i<responses.size();i++){
                    requests.put(i,"done");
                }
                this.state=COMMIT;
                return requests;
            }else {
                for(int i = 0;i<responses.size();i++){
                    if(responses.get(i).equals("ack")){
                        requests.put(i,"rollback");
                    } else {
                        if(responses.get(i).equals("nck")){
                            requests.put(i,"done");
                        }
                    }
                }
                return requests;
            }
        }
        return null;
    }
}

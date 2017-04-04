package protocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pkocsis on 31.03.17.
 */
public class StationProtocol{

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
            for(Map.Entry<Integer,String> response: responses.entrySet()){
                if(response.getValue().equals("no")){
                    failed = true;
                }
            }
            if(!failed){
                for(Map.Entry<Integer,String> response: responses.entrySet()){
                    requests.put(response.getKey(),this.sqlCommand);
                }
                this.state=COMMIT;
                return requests;
            }else {
                for(Map.Entry<Integer,String> response: responses.entrySet()){
                    requests.put(response.getKey(),"doabort");
                }
                return requests;
            }
        }
        if(this.state == COMMIT){
            HashMap<Integer,String> requests = new HashMap<>();
            boolean failed = false;
            for(Map.Entry<Integer,String> response: responses.entrySet()){
                if(response.getValue().equals("nck")){
                    failed = true;
                }
            }
            if(!failed){
                for(Map.Entry<Integer,String> response: responses.entrySet()){
                    requests.put(response.getKey(),"done");
                }
                return requests;
            }else {
                for(Map.Entry<Integer,String> response: responses.entrySet()){
                    if(response.getValue().equals("ack")){
                        requests.put(response.getKey(),"rollback");
                    } else {
                        requests.put(response.getKey(),"done");
                    }
                }
                return requests;
            }
        }
        return null;
    }
}

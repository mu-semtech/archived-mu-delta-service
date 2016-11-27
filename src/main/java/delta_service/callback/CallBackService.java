package delta_service.callback;

import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by langens-jonathan on 10.06.16.
 *
 * Callback Service
 *
 *  Allows to register different callback sets, to add callbacks to them, to call the
 *  notify for each callback registerd in the set, to check if a set exists and if a
 *  set contains callbacks.
 */
@Service
public class CallBackService
{
    // the map that contains the call back sets
    private Map<String, Set<CallBack>> callBackSets;

    /**
     * default constructor
     */
    public CallBackService()
    {
        this.callBackSets = new HashMap<String, Set<CallBack>>();
    }

    /**
     * Checks if the set is already present in the map of sets and if not it creates
     * a new one.
     * @param setName the name of the list to be added
     */
    public void addCallBackSet(String setName)
    {
        if(!this.containsCallBackList(setName))
        {
            this.callBackSets.put(setName, new HashSet<CallBack>());
        }
    }

    /**
     * checks if the map contains a sest with that name
     * @param setName the name of the set for which the inspection is required
     * @return true if the map contains a set with the specified name
     */
    public boolean containsCallBackList(String setName)
    {
        return this.callBackSets.containsKey(setName);
    }

    /**
     * Adds a call back ot the set with the given name
     * @param setName the name of the set to which the call back should be added
     * @param callBack the callback that should be added to the set
     * @throws CallBackSetNotFoundException if the set to which you want to add things isnt in the map of sets
     */
    public void addCallBack(String setName, CallBack callBack) throws CallBackSetNotFoundException
    {
        if(this.containsCallBackList(setName))
        {
            this.callBackSets.get(setName).add(callBack);
        }
        else
        {
            throw new CallBackSetNotFoundException(setName);
        }
    }

    /**
     * Calls the notify of each callback for the set with the given name. The call back will be made
     * with the presented body.
     * @param setName the name of the set for which all callbacks should be made
     * @param body the body the needs to be posted to the call back location
     * @throws CallBackSetNotFoundException if the set with the given name is not present in the map
     */
    public void notifyCallBacks(String setName, String body) throws CallBackSetNotFoundException
    {
        if(this.containsCallBackList(setName))
        {
            for(CallBack callBack : this.callBackSets.get(setName))
            {
                callBack.notify(body);
            }
        }
        else
        {
            throw new CallBackSetNotFoundException(setName);
        }
    }

    /**
     * To check if the set with the given name is present in the map of sets and, if it is, to return
     * if there are callbacks in that set
     * @param setName the name of the set
     * @return true if the set is in the map and if that set is not empty
     */
    public boolean containsCallBacksForSet(String setName)
    {
        return ((this.containsCallBackList(setName)) && (!this.callBackSets.get(setName).isEmpty()));
    }
}

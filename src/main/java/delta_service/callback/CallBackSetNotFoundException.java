package delta_service.callback;

/**
 * Created by langens-jonathan on 14.06.16.
 *
 * This exception symbolyfies that a set with the given name was not found
 */
public class CallBackSetNotFoundException extends Exception {
    public CallBackSetNotFoundException()
    {
        super("A Call Back Set was not found!");
    }

    public CallBackSetNotFoundException(String setName)
    {
        super("The Call Back Set \"" + setName + "\" was not found in the map of sets");
    }
}

package delta_service.config;

public class Configuration {
    /*
     * Describing the query endpoint
     */
    public static String queryEndpoint = "http://dbold:8890/sparql";
    public static String queryUser=null;
    public static String queryPwd=null;

    /*
     * Describing the update endpoint
     */
    public static String updateEndpoint = "http://dbold:8890/sparql";
    public static String updateUser=null;
    public static String updatePwd=null;

    /*
     * if calculateEffectives is turned off the service will be a lot more
     * perfromant. the consumers of the service will however not be informed
     * that effective differences are not calculated, they just are NOT.
     */
    public static boolean calculateEffectives = true;
}

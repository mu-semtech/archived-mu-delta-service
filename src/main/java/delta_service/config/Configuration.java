package delta_service.config;

import jdk.nashorn.internal.runtime.regexp.joni.Config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Configuration {
    /*
     * Logging etc
     */
    public static boolean logAllQueries = true;
    public static boolean logImportantQueries = true;
    public static boolean logDeltaResults = true;

    /*
     * Describing the query endpoint
     */
    public static String queryEndpoint = "http://db:8890/sparql";
    public static String queryUser=null;
    public static String queryPwd=null;

    /*
     * Describing the update endpoint
     */
    public static String updateEndpoint = "http://db:8890/sparql";
    public static String updateUser=null;
    public static String updatePwd=null;

    /*
     * if calculateEffectives is turned off the service will be a lot more
     * perfromant. the consumers of the service will however not be informed
     * that effective differences are not calculated, they just are NOT.
     */
    public static boolean calculateEffectives = true;

    private static Properties properties = null;

    private static Properties getProperties()
    {
        if(Configuration.properties == null)
        {
            FileInputStream input;
            try
            {
                String filename = System.getenv("CONFIGFILE");
                System.out.println(filename);
                input = new FileInputStream(filename);
                Configuration.properties = new Properties();
                Configuration.properties.load(input);
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        return Configuration.properties;
    }

    public static String getProperty(String name)
    {
        return Configuration.getProperties().getProperty(name);
    }
}

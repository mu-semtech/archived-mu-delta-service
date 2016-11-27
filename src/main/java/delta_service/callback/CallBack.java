package delta_service.callback;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

/**
 * Created by langens-jonathan on 10.06.16.
 *
 * A callback has a url to which it will try to connect and on which
 * it will perform then POST requests with a given body. The body is
 * presented raw.
 */
public class CallBack
{
    // the location to which the call back needs to be made
    private String url;

    /**
     * default getter for the url
     * @return this.url
     */
    public String getUrl() {
        return url;
    }

    /**
     * default setter for the url
     * @param url the url to which this call back needs to perform its requests
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * notify will perform a POST request to this.url and send the passed body-parameter raw as
     * the request body.
     *
     * @param body the raw form of the request body
     */
    public void notify(String body)
    {
        try {
            URL u = new URL(this.url);
            byte[] postData       = body.getBytes( "UTF-8" );
            int    postDataLength = postData.length;
            HttpURLConnection connection = (HttpURLConnection) u.openConnection();

            connection.setRequestMethod("POST");
            connection.setRequestProperty( "Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty( "charset", "utf-8");
            connection.setRequestProperty( "Content-Length", Integer.toString( postDataLength ));
            connection.setInstanceFollowRedirects( false );
            connection.setUseCaches( false );

            connection.setDoOutput(true);

            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            wr.write(postData);
            wr.flush();

            connection.getResponseCode();
        }
        catch (ProtocolException e)
        {
            System.out.println("[!] Caught protocol exception, stack trace:");
            e.printStackTrace();
        }
        catch(MalformedURLException e)
        {
            System.out.println("[!] Malformed URL: " + this.url);
            e.printStackTrace();
        }
        catch(IOException e)
        {
            System.out.println("[!] Could not connect...");
            e.printStackTrace();
        }
    }

    /**
     * class comparator. will return true if the passed object is of type call back
     * and if it's url matches this url
     * @param o the object for which equality should be checked
     * @return true if the passed object is logically the same as this object
     */
    @Override
    public boolean equals(Object o)
    {
        if(o.getClass().equals(CallBack.class))
        {
            if(this.url.equals(((CallBack)o).getUrl()))
            {
                return true;
            }
        }

        return false;
    }

    /**
     * class hash code generator
     * @return the hash code for the url string
     */
    @Override
    public int hashCode()
    {
        return this.url.hashCode();
    }
}

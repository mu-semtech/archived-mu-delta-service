package delta_service.query;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.http.HTTPRepository;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;

/**
 * Created by jonathan-langens on 3/4/16.
 */
public class SPARQLService
{
    /**
     * this sends a GET request to the given URL and produces a list of triple objects
     * that match the query.
     *
     * the url is supposed to be formatted as follows
     *   http://localhost:8890/sparql?query=URL_ENCODED_SELECT_QUERY
     *
     * TODO this method is still quite ugly, the main reason for implementing this is that
     * TODO the sesame library does not allow you to post a query with 2 graphs
     *
     * TODO this method is also not generic and will only work for the specific use case for which it was implemented
     *
     * @param url a fully url-endpoint with query url that you would use to do a GET with postman
     * @return a list of triples that were returned by the SPARQL endpoint
     * @throws MalformedURLException if the URL cannot be passed to the constructor of a java.util.URL object
     * @throws IOException if the connection to the SPARQL enpoint cannot be opened
     */
    @SuppressWarnings("unchecked")
    public List<Triple> getTriplesViaGet(String url) throws MalformedURLException, IOException
    {
        URL u = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) u.openConnection();

        // just want to do an HTTP GET here
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");

        // uncomment this if you want to write output to this url
        //connection.setDoOutput(true);

        // give it 15 seconds to respond
        connection.setReadTimeout(15*1000);
        connection.connect();

        BufferedReader reader = null;
        StringBuilder stringBuilder;

        // read the output from the server
        reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        stringBuilder = new StringBuilder();

        String line = null;
        while ((line = reader.readLine()) != null)
        {
            stringBuilder.append(line + "\n");
        }
        String jsonString = stringBuilder.toString();

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue(jsonString, Map.class);


        List l =((List<Map<String,Object>>)((Map) jsonMap.get("results")).get("bindings"));
        List<Triple> triples = new ArrayList<Triple>();

        for(Object tripleMap : l)
        {
            Map<String, Object> cTripleMap = (Map<String, Object>) tripleMap;
            
            Map<String, Object> sMap = (Map<String, Object>) cTripleMap.get("s");
            String sValue = (String) sMap.get("value");
            
            Map<String, Object> pMap = (Map<String, Object>) cTripleMap.get("p");
            String pValue = (String) pMap.get("value");

            Map<String, Object> oMap = (Map<String, Object>) cTripleMap.get("o");
            String oValue = (String) oMap.get("value");
            String oType = (String) oMap.get("datatype");
            if(oType == null)
                oType = (String) oMap.get("type");
            

            Triple t = new Triple();

            // first extract the subject
            t.setSubject(sValue);

            // then the predicate
            t.setPredicate(pValue);

            // and finally the object
            t.setObjectString(oValue);
            t.setObjectType(oType);

            // add it to the triples
            triples.add(t);
        }

        return triples;
    }

    @SuppressWarnings("unchecked")
    public String getSPARQLResponse(String url, Map<String,String> headers) throws MalformedURLException, IOException
    {
        URL u = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) u.openConnection();

        // just want to do an HTTP GET here
        connection.setRequestMethod("GET");

        String [] blackList = {"accept-encoding"};

        //connection.setRequestProperty("Accept", "application/json");
        for(String headerName : headers.keySet())
        {
            boolean blackListed = false;
            for(String b : blackList)
            {
                if(headerName.toLowerCase().equals(b.toLowerCase()))
                    blackListed = true;
            }
            if(!blackListed)
                connection.setRequestProperty(headerName, headers.get(headerName));
        }

        // give it 15 seconds to respond
        connection.setReadTimeout(15*1000);
        connection.connect();

        BufferedReader reader = null;
        StringBuilder stringBuilder;

        // read the output from the server
        reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        stringBuilder = new StringBuilder();

        String line = null;
        while ((line = reader.readLine()) != null)
        {
            stringBuilder.append(line + "\n");
        }
        return stringBuilder.toString();
    }

    @SuppressWarnings("unchecked")
    public String postSPARQLResponse(String url, String query, Map<String, String> headers) throws MalformedURLException, IOException
    {
        URL obj = new URL(url);

        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        //add reuqest header
        con.setRequestMethod("POST");
        //con.setRequestProperty("Accept", "application/json");

        String [] blackList = {"accept-encoding"};

        //connection.setRequestProperty("Accept", "application/json");
        for(String headerName : headers.keySet())
        {
            boolean blackListed = false;
            for(String b : blackList)
            {
                if(headerName.toLowerCase().equals(b.toLowerCase()))
                    blackListed = true;
            }
            if(!blackListed)
                con.setRequestProperty(headerName, headers.get(headerName));
        }

        // Send post request
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(query);
        wr.flush();
        wr.close();

        int responseCode = con.getResponseCode();

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        System.out.println(response.toString());

        return (response.toString());
    }

    /**
     * this sends a GET request to the given URL and produces a list of triple objects
     * that match the query.
     *
     * the url is supposed to be formatted as follows
     *   http://localhost:8890/sparql?query=URL_ENCODED_SELECT_QUERY
     *
     * TODO this method is still quite ugly, the main reason for implementing this is that
     * TODO the sesame library does not allow you to post a query with 2 graphs
     *
     * TODO this method is also not generic and will only work for the specific use case for which it was implemented
     *
     * @param url a fully url-endpoint with query url that you would use to do a GET with postman
     * @return a list of triples that were returned by the SPARQL endpoint
     * @throws MalformedURLException if the URL cannot be passed to the constructor of a java.util.URL object
     * @throws IOException if the connection to the SPARQL enpoint cannot be opened
     */
    @SuppressWarnings("unchecked")
    public List<Triple> getTriplesViaConstruct(String url) throws MalformedURLException, IOException
    {
        URL u = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) u.openConnection();

        // just want to do an HTTP GET here
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");

        // uncomment this if you want to write output to this url
        //connection.setDoOutput(true);

        // give it 15 seconds to respond
        connection.setReadTimeout(15*1000);
        connection.connect();

        BufferedReader reader = null;
        StringBuilder stringBuilder;

        // read the output from the server
        reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        stringBuilder = new StringBuilder();

        String line = null;
        while ((line = reader.readLine()) != null)
        {
            stringBuilder.append(line + "\n");
        }
        String jsonString = stringBuilder.toString();

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue(jsonString, Map.class);

        List<Triple> triples = new ArrayList<Triple>();

        for(String uri : jsonMap.keySet())
        {
            Map<String, Object> poMap = (Map<String, Object>)jsonMap.get(uri);
            for(String pred : poMap.keySet())
            {
                List<Map<String, Object>> oMapList = (List<Map<String, Object>>) poMap.get(pred);
                for(Object oMapObject : oMapList.toArray()) {
                    Map<String, Object> oMap = (Map<String, Object>)oMapObject;
                    Triple triple = new Triple();
                    triple.setSubject(uri);
                    triple.setPredicate(pred);
                    triple.setObjectString((String) oMap.get("value"));
                    triple.setObjectType((String) oMap.get("type"));
                    if (oMap.containsKey("lang")) {
                        triple.setObjectString((String) oMap.get("value") + "@" + (String) oMap.get("lang"));
                    }
                    triples.add(triple);
                }
            }

        }
        return triples;
    }

}
package delta_service.query;


import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
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

        connection.disconnect();

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
    public Response getSPARQLResponse(String url, Map<String,String> headers) throws MalformedURLException, IOException
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

        connection.disconnect();

        Response response = new Response();

        response.responseText = stringBuilder.toString();
        for(String header : connection.getHeaderFields().keySet())
        {
            // don't look at me, this is spring's fault!
            if(header != null)response.responseHeaders.put(header, connection.getHeaderField(header));
        }

        return response;
    }

    @SuppressWarnings("unchecked")
    public Response postSPARQLResponse(String url, String query, Map<String, String> headers) throws MalformedURLException, IOException
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
        if(headers.containsKey("content-type") && ((String)headers.get("content-type")).equalsIgnoreCase("application/x-www-form-urlencoded"))
        {
            // should url encode the query and assign it
            wr.writeBytes("query=" + URLEncoder.encode(query, "UTF-8"));
        }
        else {
            wr.writeBytes(query);
        }
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
        con.disconnect();

        Response toReturn = new Response();

        toReturn.responseText = response.toString();

        for(String header : con.getHeaderFields().keySet())
        {
            // don't look at me, this is spring's fault!
            if(header != null)toReturn.responseHeaders.put(header, con.getHeaderField(header));
        }

        return toReturn;
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

        connection.disconnect();

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

                    // check if the thing we get out is an short/integer/long/float/double
                    if((oMap.get("value")).getClass().equals(Short.class) ||
                            (oMap.get("value")).getClass().equals(Integer.class) ||
                            (oMap.get("value")).getClass().equals(Long.class) ||
                            (oMap.get("value")).getClass().equals(Float.class) ||
                            (oMap.get("value")).getClass().equals(Double.class)) {
                        triple.setObjectString("" + oMap.get("value"));
                    }
                    else {
                        // TODO this is a hack because the Jackson Library on it's own decides to replaces
                        // TODO all "\\n" sequences with "\n". The reason why I can do this relatively safely
                        // TODO is because virtuoso (did not check OWLIM for that matter) does not allow newlines
                        // TODO in a literal
                        CharSequence unescaped = "\"";
                        CharSequence escaped = "\\\"";
                        triple.setObjectString(((String) oMap.get("value"))
                                .replace("\n", "\\n")
                                .replace(unescaped, escaped)
                                .replace("\'", "\'")
                                .replace("\r", "\\r")
                                .replace("\t", "\\t")
                        );
                    }
                    if(((String) oMap.get("type")).equalsIgnoreCase("uri"))
                        triple.setObjectIsURI(true);
                    if (oMap.containsKey("lang")) {
                        triple.setObjectLanguage((String) oMap.get("lang"));
                    }
                    if (oMap.containsKey("datatype")) {
                        triple.setObjectType((String) oMap.get("datatype"));
                    }
                    triples.add(triple);
                }
            }

        }
        return triples;
    }


    /**
     * this sends a POST request to the given URL and produces a list of triple objects
     * that match the query.
     *
     * @param url endpoint of your DB
     * @param query query=#{encodedQuery}
     * @return a list of triples that were returned by the SPARQL endpoint
     * @throws MalformedURLException if the URL cannot be passed to the constructor of a java.util.URL object
     * @throws IOException if the connection to the SPARQL enpoint cannot be opened
     */
    @SuppressWarnings("unchecked")
    public List<Triple> getTriplesViaPostConstruct(String url, String query) throws MalformedURLException, IOException
    {

        URL u = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) u.openConnection();

        // uncomment this if you want to write output to this url
        connection.setDoOutput(true);
        // just want to do an HTTP POST here
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Accept", "application/json");

        // Writing the post data to the HTTP request body
        BufferedWriter httpRequestBodyWriter = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
        httpRequestBodyWriter.write(query);
        httpRequestBodyWriter.close();

        // give it 15 seconds to respond
        connection.setReadTimeout(15*1000);

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

        connection.disconnect();

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

                    // check if the thing we get out is an short/integer/long/float/double
                    if((oMap.get("value")).getClass().equals(Short.class) ||
                        (oMap.get("value")).getClass().equals(Integer.class) ||
                        (oMap.get("value")).getClass().equals(Long.class) ||
                        (oMap.get("value")).getClass().equals(Float.class) ||
                        (oMap.get("value")).getClass().equals(Double.class)) {
                        triple.setObjectString("" + oMap.get("value"));
                    }
                    else {
                        // TODO this is a hack because the Jackson Library on it's own decides to replaces
                        // TODO all "\\n" sequences with "\n". The reason why I can do this relatively safely
                        // TODO is because virtuoso (did not check OWLIM for that matter) does not allow newlines
                        // TODO in a literal
                        CharSequence unescaped = "\"";
                        CharSequence escaped = "\\\"";
                        triple.setObjectString(((String) oMap.get("value"))
                            .replace("\n", "\\n")
                            .replace(unescaped, escaped)
                            .replace("\'", "\'")
                            .replace("\r", "\\r")
                            .replace("\t", "\\t")
                        );
                    }
                    if(((String) oMap.get("type")).equalsIgnoreCase("uri"))
                        triple.setObjectIsURI(true);
                    if (oMap.containsKey("lang")) {
                        triple.setObjectLanguage((String) oMap.get("lang"));
                    }
                    if (oMap.containsKey("datatype")) {
                        triple.setObjectType((String) oMap.get("datatype"));
                    }
                    triples.add(triple);
                }
            }

        }
        return triples;
    }

}

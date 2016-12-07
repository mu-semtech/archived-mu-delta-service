package delta_service.web;

import SPARQLParser.SPARQL.InvalidSPARQLException;
import SPARQLParser.SPARQL.SPARQLQuery;
import com.fasterxml.jackson.databind.ObjectMapper;
import delta_service.config.Configuration;
import delta_service.query.QueryInfo;
import delta_service.query.QueryService;
import delta_service.query.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

@RestController
public class RootController {

  @Inject
  private QueryService queryService;

    private static final Logger log = LoggerFactory.getLogger(RootController.class);

  /**
   * initializes the callback service with 2 call back sets (allDifferences and effectiveDifferences)
   */
  @PostConstruct
  public void init()
  {
  }

  /**
   * Auto wired web entry point
   *
   * expects a body in the form
   * {
   *     "callback":"<CALLBACKLOCATION>"
   * }
   *
   * a Call Back object with this location is instantiated and added to the all differences set
   * @param request
   * @param response
   * @param body
     * @return
     */
  @RequestMapping(value = "/registerForPotentialDifferences")
  public ResponseEntity<String> registerAD(HttpServletRequest request, HttpServletResponse response, @RequestBody(required = false) String body)
  {
    Map<String, Object> jsonMap;
        try {
          ObjectMapper mapper = new ObjectMapper();
          jsonMap = mapper.readValue(body, Map.class);
          String callbackString = (String)jsonMap.get("callback");
            this.queryService.addCallBack("potentialDifferences", callbackString);
    }
    catch(IOException e)
    {
      e.printStackTrace();
    }

    return new ResponseEntity<String>("OK", HttpStatus.OK);
  }

  /**
   * Auto wired web entry point
   *
   * expects a body in the form
   * {
   *     "callback":"<CALLBACKLOCATION>"
   * }
   *
   * a Call Back object with this location is instantiated and added to the effective differences set
   * @param request
   * @param response
   * @param body
   * @return
   */
  @RequestMapping(value = "/registerForEffectiveDifferences")
  public ResponseEntity<String> registerED(HttpServletRequest request, HttpServletResponse response, @RequestBody(required = false) String body)
  {
    Map<String, Object> jsonMap;
    try {
      ObjectMapper mapper = new ObjectMapper();
      jsonMap = mapper.readValue(body, Map.class);
      String callbackString = (String)jsonMap.get("callback");
        this.queryService.addCallBack("effectiveDifferences", callbackString);
    }
    catch(IOException e)
    {
      e.printStackTrace();
    }

    return new ResponseEntity<String>("OK", HttpStatus.OK);
  }

    /**
     * TODO: Add more supported content types there is a problem with the text/turtle content-type
     * TODO: for some reason the StringHttpMessageConverter barfs on it...
     * @param request
     * @param response
     * @param body
     * @return
     * @throws InvalidSPARQLException
     */
  @RequestMapping(value = "/sparql", produces = {"application/sparql-results+xml", "application/sparql-results+json", "text/html"})
  public ResponseEntity<String> preProcessQuery(HttpServletRequest request, HttpServletResponse response, @RequestBody(required = false) String body) throws InvalidSPARQLException
  {
    try {
         /*
         * Getting the query string,... somehow
         */

        String queryString;

        if (request.getParameterMap().containsKey("query")) {
            queryString = request.getParameter("query");
            try {
                queryString = URLDecoder.decode(queryString, "UTF-8");
            }catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        else
        {
            queryString = URLDecoder.decode(body, "UTF-8");
            if(queryString.toLowerCase().startsWith("update="))
            {
                queryString = queryString.substring(7, queryString.length());
            }
            if(queryString.toLowerCase().startsWith("query="))
            {
                queryString = queryString.substring(6, queryString.length());
            }
        }

        /*
         * Getting the headers ... somehow
         */
        Map<String, String> headers = new HashMap<String, String>();
        Enumeration<String> henum = request.getHeaderNames();
        while(henum.hasMoreElements())
        {
            String headerName = henum.nextElement();
            headers.put(headerName, request.getHeader(headerName));
        }

        /*
         * if UPDATE then ...
         */
        SPARQLQuery.Type queryType = null;
        try {
            queryType = SPARQLQuery.extractType(queryString);
        }catch(InvalidSPARQLException invalidSPARQLException)
        {
            invalidSPARQLException.printStackTrace();
        }
        if(queryType.equals(SPARQLQuery.Type.UPDATE)) {

            /*
             * Getting the parsed query object ... somehow
             */
            SPARQLQuery parsedQuery = new SPARQLQuery(queryString);

            // prepare the query object
            QueryInfo queryInfo = new QueryInfo();
            queryInfo.headers = headers;
            queryInfo.endpoint = Configuration.updateEndpoint;
            queryInfo.originalQuery = queryString;
            queryInfo.query = parsedQuery;

            // register it for processing
            this.queryService.registerUpdateQuery(queryInfo);

            // while it has not been process ... wait
            while(this.queryService.getProcessedQueries().contains(queryInfo) == false)
            {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // extract the response and remove all other info
            String queryResponse = queryInfo.response.responseText;
            this.queryService.getProcessedQueries().remove(queryInfo);

            // setting the headers...
            if(queryInfo.response.responseHeaders != null)
            {
                for(String header : queryInfo.response.responseHeaders.keySet())
                {
                    response.setHeader(header, queryInfo.response.responseHeaders.get(header));
                }
            }

            // and then return the result
            return new ResponseEntity<String>(queryInfo.response.responseText, HttpStatus.OK);
        }

        /**
         * If we are dealing with a SELECT query we can just return the response as is...
         */
        if(!queryType.equals(SPARQLQuery.Type.UPDATE))
        {
            Response sparqlResponse = this.queryService.sparqlService.getSPARQLResponse(Configuration.queryEndpoint + "?query=" + URLEncoder.encode(queryString, "UTF-8"), headers);
            String qrp = sparqlResponse.responseText;
            for(String header:sparqlResponse.responseHeaders.keySet())
            {
                response.setHeader(header, sparqlResponse.responseHeaders.get(header));
            }
            return new ResponseEntity<String>(qrp, HttpStatus.OK);
        }

    }catch(InvalidSPARQLException e)
    {
      e.printStackTrace();
    } catch (MalformedURLException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

      return ResponseEntity.ok("");
  }


}

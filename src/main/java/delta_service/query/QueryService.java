package delta_service.query;

import SPARQLParser.SPARQL.InvalidSPARQLException;
import SPARQLParser.SPARQL.SPARQLQuery;
import SPARQLParser.SPARQLStatements.BlockStatement;
import SPARQLParser.SPARQLStatements.IStatement;
import SPARQLParser.SPARQLStatements.UpdateBlockStatement;
import delta_service.callback.CallBack;
import delta_service.callback.CallBackService;
import delta_service.callback.CallBackSetNotFoundException;
import delta_service.config.Configuration;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;

/**
 * Created by langens-jonathan on 31.05.16.
 *
 * The Query service has only 1 method: the getDifferenceTriples method that will return a map
 * with deltas for all graphs on which the passed query has an effect.
 */
@Service
public class QueryService
{
    public SPARQLService sparqlService;
    private CallBackService callBackService;

    private List<QueryInfo> updateQueries = new ArrayList<QueryInfo>();
    public List<QueryInfo> processedQueries = new ArrayList<QueryInfo>();

    private boolean isProcessingUpdateQueries = false;

    public QueryService() {
        this.sparqlService = new SPARQLService();

        this.callBackService = new CallBackService();
        this.callBackService.addCallBackSet("allDifferences");
        this.callBackService.addCallBackSet("potentialDifferences");
        this.callBackService.addCallBackSet("effectiveDifferences");
    }

    public Response postSPARQLResponse(String location, String query, Map<String, String> headers) throws IOException
    {
        return this.sparqlService.postSPARQLResponse(location, query, headers);
    }

    public void addCallBack(String setName, String callBackLocation)
    {
        CallBack callback = new CallBack();
        callback.setUrl(callBackLocation);
        try {
            this.callBackService.addCallBack(setName, callback);
        } catch (CallBackSetNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void notifyCallBacks(String setname, String message)
    {
        try {
            this.callBackService.notifyCallBacks(setname, message);
        } catch (CallBackSetNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void registerUpdateQuery(QueryInfo sparqlQuery)
    {
        this.updateQueries.add(sparqlQuery);
        this.startProcessingUpdateQueries();
    }

    public void processNextQuery()
    {
        if(this.updateQueries.size() > 0) {
            this.isProcessingUpdateQueries = true;
            QueryInfo query = this.updateQueries.get(0);
            try {
                this.processUpdateQuery(query);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            finally {
                this.updateQueries.remove(query);
                this.processedQueries.add(query);
                if (this.updateQueries.size() > 0) {
                    this.startProcessingUpdateQueries();
                } else {
                    this.isProcessingUpdateQueries = false;
                }
            }
        }
    }

    public void startProcessingUpdateQueries()
    {
        if(/*this.isProcessingUpdateQueries == false*/1 == 1)
        {
            this.processNextQuery();
        }
    }

    public void processUpdateQuery(QueryInfo queryInfo) throws InvalidSPARQLException, IOException {
//        try {
            // 1. calculate the difference triples (for this we want the state of the DB as before the update)
            SPARQLQuery parsedQuery = queryInfo.query;
            Map<String, DifferenceTriples> diff = this.getDifferenceTriples(parsedQuery);

            // 2. prepare the JSON responses
            String potJson = "{\"query\":\"" + URLEncoder.encode(queryInfo.originalQuery, "UTF-8") + "\", \"delta\":[";
            String effectiveJson = "{\"query\":\"" + URLEncoder.encode(queryInfo.originalQuery, "UTF-8") + "\", \"delta\":[";

            for (String g : diff.keySet()) {
                potJson += "{\"graph\":\"" + g + "\",\"delta\":" + diff.get(g).getPotentialChangesAsJSON() + "},";
                effectiveJson += "{\"graph\":\"" + g + "\",\"delta\":" + diff.get(g).getEffectiveChangesAsJSON() + "},";
            }

            if (!diff.keySet().isEmpty()) {
                potJson = potJson.substring(0, potJson.length() - 1);
                effectiveJson = effectiveJson.substring(0, effectiveJson.length() - 1);
            }

            potJson += "]}";
            effectiveJson += "]}";

            // 3. perform the actual query on the DB
            queryInfo.response = this.postSPARQLResponse(queryInfo.endpoint, queryInfo.originalQuery, queryInfo.headers);

            // 4. notify the callback endpoints
            this.notifyCallBacks("effectiveDifferences", effectiveJson);
            this.notifyCallBacks("potentialDifferences", potJson);

            this.isProcessingUpdateQueries = false;
    }

    public QueryService(SPARQLService service){this.sparqlService = service;}

    /**
     * Returns a map that projects graph names on DifferenceTriples-objects. Those DifferenceTriples-objects
     * contain 4 sets, 2 of potential insert and delete triples and 2 with effective insert and delete
     * triples.
     *
     * note for debugging: This map is maintaided troughout the entire function, whenever a set of triples is
     * requested for a certain graph it will be returned from this map, if that graph is not yet present as a
     * key in the map a new differenceTriples object will be added to the map for that graph.
     *
     * @param parsedQuery a SPARQLQuery object that has been initialized with a query
     * @return a map of graph names and difference triple objects (Map<String, DifferenceTriples>)
     * @throws InvalidSPARQLException if the parsedQuery object does not contain a valid SPARQL query we throw
     *         an exception.
     */
    public Map<String, DifferenceTriples> getDifferenceTriples(SPARQLQuery parsedQuery) throws InvalidSPARQLException, IOException {
        // the deltas for each query will be stored in a map the projects each graph on a differenceTriples object
        Map<String, DifferenceTriples> differenceTriplesMap = new HashMap<String, DifferenceTriples>();

        /*
         * we build a queryPrefix quickly because this will be necessary for almost every subsequent query
         * that we will construct.
         */
        String queryPrefix = "";
        for(String key : parsedQuery.getPrefixes().keySet())
        {
            queryPrefix += "PREFIX " + key + ": <" + parsedQuery.getPrefixes().get(key) + ">\n";
        }

        /*
         * we clone the query as to ensure that the original query object remains untouched
         */
        SPARQLQuery clonedQuery = parsedQuery.clone();

        /*
         * we loop over the blocks in the query, for every block the idea is:
         *  1. find out on which graph it operates (if none than it's the graph for the entire query
         *  2. remove all graph statements for the block
         *  3. transform it into a construct
         *  4. whatever comes out will be the potential difference triples
         */
        for(IStatement statement : clonedQuery.getStatements()) {
            // check if the block is an update (otherwise it won't generate a delta)
            if (statement.getType().equals(IStatement.StatementType.UPDATEBLOCK)) {
                // cast it, the updateblockstatement has functional support that will be handy later on
                UpdateBlockStatement updateBlockStatement = (UpdateBlockStatement)statement;

                /*
                 * step 1. finding the graph
                 */
                String originalGraph = updateBlockStatement.getGraph();

                if(originalGraph.isEmpty())
                    originalGraph = parsedQuery.getGraph();

                /*
                 * step 2. replacing all graph statements in the block
                 */
                updateBlockStatement.replaceGraphStatements("");

                /*
                 * step 3. transforming it into a construct
                 */
                String extractQuery = queryPrefix + "WITH <" + originalGraph + ">\n";
                extractQuery += "CONSTRUCT\n{\n";

                for(IStatement innerStatement : updateBlockStatement.getStatements())
                {
                    extractQuery += innerStatement.toString() + "\n";
                }

                extractQuery += "}\nWHERE\n{\n";

                if(updateBlockStatement.getWhereBlock() != null) {
                    for (IStatement whereStatement : updateBlockStatement.getWhereBlock().getStatements()) {
                        extractQuery += whereStatement.toString() + "\n";
                    }
                }

                extractQuery += "}";

                /*
                 * step 4. transforming the construct query in a set of triples, we will automatically
                 *         add the triples to the differenceTriples object for that graph in the map
                 */
                Set<Triple> insertTriples = null;
                Set<Triple> deleteTriples = null;

                if(!differenceTriplesMap.containsKey(originalGraph))
                    differenceTriplesMap.put(originalGraph, new DifferenceTriples());

                insertTriples = differenceTriplesMap.get(originalGraph).getAllInsertTriples();
                deleteTriples = differenceTriplesMap.get(originalGraph).getAllDeleteTriples();

                /*
                 * TODO: make sure that the authentication headers are set and passed if Configuration has them
                 * for query user and pwd.
                 */
                List<Triple> triples = this.sparqlService.getTriplesViaConstruct(Configuration.queryEndpoint + "?query=" + URLEncoder.encode(extractQuery, "UTF-8"));

                if(updateBlockStatement.getUpdateType().equals(BlockStatement.BLOCKTYPE.INSERT))
                {
                    insertTriples.addAll(triples);
                }
                else
                {
                    deleteTriples.addAll(triples);
                }
            }
        }

        /**
         * Now that we have all potential inserts and deletes we still have to calculate the effective
         * versions of those sets. For this we will separate the logic in 2 different blocks, 1 for
         * deletes and 1 for insterts.
         *
         * TODO: Though the only thing that is different is the eventual calculating query so this could probably be refactored into something smaller
         * TODO: now we build 1 insert query for the triples into the respective temporary graphs, if the amount of triples is big this will be a problem
         *
         * For the deletes the strategy will be for each graph that has a delta object in our map:
         * 1. constructing an insert query to insert all potential deletes into a delete graph
         * 2. executing that query
         * 3. constructing a query that calculates the union between the tmp graph and the graph on which the original
         *    block operated
         * 4. store the result of the last query in the effective deletes for that graph
         */
        for(String graph : differenceTriplesMap.keySet()) {
            /*
             * step 1. constructing the tmp insert query that will insert the potential deletes into a tmp graph
             */
            // first we try to get the delete triples for that graph
            DifferenceTriples differenceTriples = differenceTriplesMap.get(graph);

            Set<Triple> deleteTriples = differenceTriplesMap.get(graph).getAllDeleteTriples();

            // now insert the delete triples in a temporary graph
            String deleteGraph = "<http://tmp-delete-graph>";

            // first clear the graph
            HashMap<String, String> headers = new HashMap<String, String>();
            headers.put("Content-type", "application/sparql-update");

            /*
             * TODO set authentication headers if info is available in Configuration
             */
            this.sparqlService.postSPARQLResponse(Configuration.updateEndpoint, "with " + deleteGraph + " delete {?s ?p ?o} where {?s ?p ?o.}", headers);

            String tmpDeleteInsert = queryPrefix + "\n with " + deleteGraph + "\ninsert data\n{\n";
            for (Triple t : deleteTriples)
                if (t.getObjectType().endsWith(".org/2001/XMLSchema#string>"))
                    tmpDeleteInsert += "<" + t.getSubject().substring(0, t.getSubject().length()) + "> <" + t.getPredicate() + "> " + t.getObjectAsString() + " .\n";
                else
                    tmpDeleteInsert += "<" + t.getSubject().substring(0, t.getSubject().length()) + "> <" + t.getPredicate() + "> " + t.getObjectAsString() + " .\n";

            tmpDeleteInsert += "}";

            /*
             * step 2. executing that query
             */
            this.sparqlService.postSPARQLResponse(Configuration.updateEndpoint,tmpDeleteInsert, headers);

            /*
             * step 3. constructing the union query
             */
            String unionQuery = "SELECT ?s ?p ?o WHERE { GRAPH <" + graph + "> { ?s ?p ?o . } .\n GRAPH " + deleteGraph + " { ?s ?p ?o . } .\n}";

            List<Triple> confirmedDeletes = new ArrayList<Triple>();

            String url = Configuration.queryEndpoint + "?query=" + URLEncoder.encode(unionQuery, "UTF-8");
            confirmedDeletes = this.sparqlService.getTriplesViaGet(url);

            /*
             * step 4. storing the result
             */
            differenceTriples.setEffectiveDeleteTriples(new HashSet<Triple>(confirmedDeletes));
        }

        /*
         * The effective inserts will be calculated in an analog fashion:
         * 1. constructing and insertquery
         * 2. executing it
         * 3. now we wil construct a difference query
         * 4. store the result in the correct hash
         */
        for(String graph : differenceTriplesMap.keySet()) {
            /*
             * step 1. constructing the insert query
             */
            DifferenceTriples differenceTriples = differenceTriplesMap.get(graph);

            Set<Triple> insertTriples = differenceTriplesMap.get(graph).getAllInsertTriples();

            // now insert the delete triples in a temporary graph
            String insertGraph = "<http://tmp-insert-graph>";

            // first clear the graph
            HashMap<String, String> headers = new HashMap<String, String>();
            headers.put("Content-type", "application/sparql-update");
            this.sparqlService.postSPARQLResponse(Configuration.updateEndpoint, "with " + insertGraph + " delete {?s ?p ?o} where {?s ?p ?o.}", headers);

            String tmpInsertInsert = queryPrefix + "\n with " + insertGraph + "\ninsert data\n{\n";
            for (Triple t : insertTriples)
                if (t.getObjectType().endsWith(".org/2001/XMLSchema#string>"))
                    tmpInsertInsert += "<" + t.getSubject().substring(0, t.getSubject().length()) + "> <" + t.getPredicate() + "> " + t.getObjectAsString() + " .\n";
                else
                    tmpInsertInsert += "<" + t.getSubject().substring(0, t.getSubject().length()) + "> <" + t.getPredicate() + "> " + t.getObjectAsString() + " .\n";

            tmpInsertInsert += "}";

            /*
             * step 2. executing tht query
             */
            this.sparqlService.postSPARQLResponse(Configuration.updateEndpoint, tmpInsertInsert, headers);

            /*
             * step 3. creating the difference query
             */
            /*
             * TODO I guess I should assume the graph to be mu.semte.ch/application, this should be the original query's
             * target graph!
             */
            String differenceQuery = "SELECT ?s ?p ?o WHERE {graph " + insertGraph + " {?s ?p ?o.}.\nminus\n{\ngraph <" + "http://mu.semte.ch/application" + "> {?s ?p ?o.}.}\n}";

            List<Triple> confirmedInserts = new ArrayList<Triple>();

            String url = Configuration.queryEndpoint + "?query=" + URLEncoder.encode(differenceQuery, "UTF-8");
            confirmedInserts = this.sparqlService.getTriplesViaGet(url);

            /*
             * step 4. storing the result
             */
            differenceTriples.setEffectiveInsertTriples(new HashSet<Triple>(confirmedInserts));
        }

        return differenceTriplesMap;
    }
}

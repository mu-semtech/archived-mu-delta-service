package delta_service.query;

import SPARQLParser.SPARQL.SPARQLQuery;

import java.util.Map;
import java.util.UUID;

/**
 * Created by langens-jonathan on 07.11.16.
 * TODO make this a real class, for now this is more a struct then anything else
 *
 * The main idea behind this class is that it bundles all info about a certain update query that
 * might or might not be in the update pipeline
 */
public class QueryInfo {
    public SPARQLQuery query;
    public String originalQuery;
    public Map<String, String> headers;
    public String endpoint;
    public Response response;
    public String id;

    public QueryInfo()
    {
        this.id = UUID.randomUUID().toString();
    }
}

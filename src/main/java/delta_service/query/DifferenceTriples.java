package delta_service.query;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by langens-jonathan on 31.05.16.
 *
 * This is a data structure
 *
 * The class difference triples holds a set of all triples that will
 * be updated, one set for all triples that will be deleted on a
 * certain data set, on set for all triples that will effectivly be
 * updated and one set for all triples that will effectivly be deleted.
 */
public class DifferenceTriples
{
    // a set with all triples that will be updated in the store
    private Set<Triple> allInsertTriples;

    // a set with all triples that will be deleted in the store
    private Set<Triple> allDeleteTriples;

    // a set with allt triples that will EFFECTIVLY be inserted in the graph
    private Set<Triple> effectiveInsertTriples;

    // a set will all triples that will EFFECTIVLY be deleted from the graph
    private Set<Triple> effectiveDeleteTriples;

    /**
     * default constructor
     */
    public DifferenceTriples()
    {
        this.allInsertTriples = new HashSet<Triple>();
        this.allDeleteTriples = new HashSet<Triple>();
        this.effectiveInsertTriples = new HashSet<Triple>();
        this.effectiveDeleteTriples = new HashSet<Triple>();
    }

    /**
     * adds the given update triple to the set of update triples
     * @param triple
     */
    public void addAllInsertTriple(Triple triple)
    {
        this.allInsertTriples.add(triple);
    }

    /**
     * adds the given delete triple to the set of delete triples
     * @param triple
     */
    public void addAllDeleteTripel(Triple triple)
    {
        this.allDeleteTriples.add(triple);
    }

    /**
     * adds the given update triple to the set of update triples
     * @param triple
     */
    public void addEffectiveInsertTriple(Triple triple)
    {
        this.effectiveInsertTriples.add(triple);
    }

    /**
     * adds the given delete triple to the set of delete triples
     * @param triple
     */
    public void addEffectiveDeleteTripel(Triple triple)
    {
        this.effectiveDeleteTriples.add(triple);
    }

    /**
     * Helper method that formats the potential changes in JSON
     *
     * @return a JSON representation for the potential changes stored in this differenceTriples object
     */
    public String getPotentialChangesAsJSON()
    {
        String jsonString = "";

        jsonString += "{\"inserts\":[";

        for(Triple t : this.getAllInsertTriples())
        {
            String type = "uri";
            if(t.getObjectType().toLowerCase().contains("string") ||
                    t.getObjectType().toLowerCase().contains("literal"))
            {
                type = "literal";
            }
            String oString = t.getObjectString();
            if(oString.startsWith("\""))
                oString = oString.substring(1, oString.length());
            if(oString.length() > 3)
                if(oString.substring(0, oString.length() - 2).endsWith("@"))
                    oString = oString.substring(0, oString.length() - 3);
            if(oString.endsWith("\""))
                oString = oString.substring(0, oString.length() - 1);
            jsonString += "{";
            jsonString += "\"s\":{\"value\":\"" + t.getSubject() + "\", \"type\":\"uri\"},";
            jsonString += "\"p\":{\"value\":\"" +  t.getPredicate() + "\", \"type\":\"uri\"},";
            jsonString += "\"o\":{\"value\":\"" + oString + "\", \"type\":\"" + type + "\"}},";
        }

        if(!this.getAllInsertTriples().isEmpty())
        {
            jsonString = jsonString.substring(0, jsonString.length() - 1);
        }

        jsonString += "],\"deletes\":[";

        for(Triple t : this.getAllDeleteTriples())
        {
            String type = "uri";
            if(t.getObjectType().toLowerCase().contains("string") ||
                    t.getObjectType().toLowerCase().contains("literal"))
            {
                type = "literal";
            }
            String oString = t.getObjectString();
            if(oString.startsWith("\""))
                oString = oString.substring(1, oString.length());
            if(oString.length()>3)
                if(oString.substring(0, oString.length() - 2).endsWith("@"))
                    oString = oString.substring(0, oString.length() - 3);
            if(oString.endsWith("\""))
                oString = oString.substring(0, oString.length() - 1);
            jsonString += "{";
            jsonString += "\"s\":{\"value\":\"" + t.getSubject() + "\", \"type\":\"uri\"},";
            jsonString += "\"p\":{\"value\":\"" +  t.getPredicate() + "\", \"type\":\"uri\"},";
            jsonString += "\"o\":{\"value\":\"" + oString + "\", \"type\":\"" + type + "\"}},";
        }

        if(!this.getAllDeleteTriples().isEmpty())
        {
            jsonString = jsonString.substring(0, jsonString.length() - 1);
        }

        jsonString += "]}";

        return jsonString;
    }


    /**
     * Helper method that formats the effective changes in JSON
     *
     * @return a JSON representation for the effective changes stored in this differenceTriples object
     */
    public String getEffectiveChangesAsJSON()
    {
        String jsonString = "";

        jsonString += "{\"inserts\":[";

        for(Triple t : this.getEffectiveInsertTriples())
        {
            String type = "uri";
            if(t.getObjectType().toLowerCase().contains("string") ||
                    t.getObjectType().toLowerCase().contains("literal"))
            {
                type = "literal";
            }
            System.out.println(t.getObjectAsString());
            System.out.println(t.getObjectType());
            String oString = t.getObjectString();
            if(oString.startsWith("\""))
                oString = oString.substring(1, oString.length());
            if(oString.length()>3)
                if(oString.substring(0, oString.length() - 2).endsWith("@"))
                    oString = oString.substring(0, oString.length() - 3);
            if(oString.endsWith("\""))
                oString = oString.substring(0, oString.length() - 1);
            jsonString += "{";
            jsonString += "\"s\":{\"value\":\"" + t.getSubject() + "\", \"type\":\"uri\"},";
            jsonString += "\"p\":{\"value\":\"" +  t.getPredicate() + "\", \"type\":\"uri\"},";
            jsonString += "\"o\":{\"value\":\"" + oString + "\", \"type\":\"" + type + "\"}},";
        }

        if(!this.getEffectiveInsertTriples().isEmpty())
        {
            jsonString = jsonString.substring(0, jsonString.length() - 1);
        }

        jsonString += "],\"deletes\":[";

        for(Triple t : this.getEffectiveDeleteTriples())
        {
            String type = "uri";
            if(t.getObjectType().toLowerCase().contains("string") ||
                    t.getObjectType().toLowerCase().contains("literal"))
            {
                type = "literal";
            }
            String oString = t.getObjectString();
            if(oString.startsWith("\""))
                oString = oString.substring(1, oString.length());
            if(oString.length()>3)
                if(oString.substring(0, oString.length() - 2).endsWith("@"))
                    oString = oString.substring(0, oString.length() - 3);
            if(oString.endsWith("\""))
                oString = oString.substring(0, oString.length() - 1);
            jsonString += "{";
            jsonString += "\"s\":{\"value\":\"" + t.getSubject() + "\", \"type\":\"uri\"},";
            jsonString += "\"p\":{\"value\":\"" +  t.getPredicate() + "\", \"type\":\"uri\"},";
            jsonString += "\"o\":{\"value\":\"" + oString + "\", \"type\":\"" + type + "\"}},";
        }

        if(!this.getEffectiveDeleteTriples().isEmpty())
        {
            jsonString = jsonString.substring(0, jsonString.length() - 1);
        }

        jsonString += "]}";

        return jsonString;
    }

    /**
     * @return the potential insert triples
     */
    public Set<Triple> getAllInsertTriples() {
        return allInsertTriples;
    }

    /**
     * sets (overrides) the potential insert triples
     * @param allInsertTriples a set of potential insert triples
     * @pre allInsertTriples should not be null
     */
    public void setAllInsertTriples(Set<Triple> allInsertTriples) {
        this.allInsertTriples = allInsertTriples;
    }

    /**
     * @return the potential delete triples
     */
    public Set<Triple> getAllDeleteTriples() {
        return allDeleteTriples;
    }

    /**
     * sets (overrides) the potential delete triples
     * @param allDeleteTriples a set of potential delete triples
     * @pre allDeleteTriples should not be null
     */
    public void setAllDeleteTriples(Set<Triple> allDeleteTriples) {
        this.allDeleteTriples = allDeleteTriples;
    }

    /**
     * @return the effective delete triples
     */
    public Set<Triple> getEffectiveDeleteTriples() {
        return effectiveDeleteTriples;
    }

    /**
     * sets (overrides) the effective delete triples
     * @param effectiveDeleteTriples a set of effective delete triples
     * @pre effectiveDeleteTriples should not be null
     */
    public void setEffectiveDeleteTriples(Set<Triple> effectiveDeleteTriples) {
        this.effectiveDeleteTriples = effectiveDeleteTriples;
    }

    /**
     * @return returns the effective insert triples
     */
    public Set<Triple> getEffectiveInsertTriples() {
        return effectiveInsertTriples;
    }

    /**
     * sets (overrides) the effective insert triples
     * @param effectiveInsertTriples a set of effective insert triples
     * @pre effectiveInsertTriples should not be null
     */
    public void setEffectiveInsertTriples(Set<Triple> effectiveInsertTriples) {
        this.effectiveInsertTriples = effectiveInsertTriples;
    }
}

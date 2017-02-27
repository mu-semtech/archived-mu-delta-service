package SPARQLParser.SPARQLStatements;

import SPARQLParser.SPARQL.InvalidSPARQLException;
import SPARQLParser.SPARQL.SplitQuery;

import java.util.HashSet;
import java.util.Set;

/**
 * A SPARQL Update block is defined as:
 * - it starts with a '{'
 * - it ends with a '}'
 * - it may contain statements (which are just stored as Strings)
 * - it may contain innerblocks, those inner blocks look like:
 *     - GRAPH <graph> { [this is a new block] }
 */
public class UpdateBlockStatement extends BlockStatement
{
    //private List<IStatement> statements = new ArrayList<IStatement>();
    private WhereBlockStatement whereBlock = null;
    public UpdateBlockStatement(BLOCKTYPE type, String block, String graph)
    {
        this.statements.add(new SimpleStatement(block));
        this.type = type;
        this.graph = graph;
    }

    private UpdateBlockStatement()
    {

    }

    public Set<String> getUnknowns()
    {
        Set<String> u = new HashSet<String>();
        for(IStatement s : this.statements)
            u.addAll(s.getUnknowns());
        return u;
    }

    public UpdateBlockStatement(BLOCKTYPE type, SplitQuery.SplitQueryIterator iterator) throws InvalidSPARQLException
    {
        this.type = type;

        if(!iterator.hasNext())
        {
            throw new InvalidSPARQLException("Invalid SPARQL: on line " + iterator.getCurrentLine() + " near: " + iterator.getPrevious());
        }

        // who cares if it starts with insert data or insert
        if(iterator.peekNext().toLowerCase().startsWith("data"))
        {
            String data = iterator.next();
            iterator.breakOff(data.substring(0, 4));
        }

        String next = iterator.next();
        if (!next.startsWith("{")) {
            throw new InvalidSPARQLException("Invalid SPARQL on line: " + iterator.getCurrentLine() + " expected '{' at" + iterator.getPrevious());
        }
        iterator.breakOff("{");

        parseBlock(iterator);
    }

    private void parseBlock(SplitQuery.SplitQueryIterator iterator) throws InvalidSPARQLException
    {
        String block = "";

        while (iterator.hasNextIncludingNewLines()) {

            // do we get a new inner block
            if (iterator.peekNext().startsWith("}")) {
                iterator.next();
                iterator.breakOff("}");
                if(!block.trim().isEmpty()) {
                    statements.add(new SimpleStatement(block));
                }

                if(iterator.hasNext() && iterator.peekNext().toLowerCase().startsWith("insert"))
                {
                    // insert block after delete, handle it
                    String insertPart = iterator.next();
                    iterator.breakOff(insertPart.substring(0, 6));

                    // if this is not a delete block then barf...
                    if(!this.type.name().equalsIgnoreCase("delete"))
                    {
                        throw new InvalidSPARQLException("Invalid SPARQL on line: " + iterator.getCurrentLine() + " expected WHERE clause after " + this.type.name() + " clause around " + iterator.getPrevious());
                    }

                    block = "";

                    // we will put the content of this block up until this point into a new
                    // delete update block
                    String deleteContents = "";
                    for(IStatement s : this.getStatements())
                    {
                        deleteContents += "\n" + s.toString();
                    }
                    UpdateBlockStatement deleteStatement = new UpdateBlockStatement(BLOCKTYPE.DELETE, deleteContents, this.getGraph());

                    // now we clear the contents of this block, effectively putting everything in the delete block
                    this.statements.clear();
                    this.statements.add(deleteStatement);

                    // we change the type of this block to the combined DELETE_INSERT
                    // this will flag at the end of parsing this block to put everything but the first
                    // statement in an updateblock, we then still need to insert the wereblock
                    this.type = BLOCKTYPE.DELETE_INSERT;

                    // the next part now should be {
                    String insertBracket = iterator.next();
                    if(!insertBracket.startsWith("{"))
                    {
                        throw new InvalidSPARQLException("Invalid SPARQL on line: " + iterator.getCurrentLine() + " expected WHERE clause after " + this.type.name() + " clause around " + iterator.getPrevious());
                    }
                    iterator.breakOff("{");

                    // process as further as normally
                    continue;
                }

                // at this point we can check if this.type == DELETE_INSERT, if yes then we will put
                // all statements[1-END] in a separate INSERT block
                if(this.type.equals(BLOCKTYPE.DELETE_INSERT))
                {
                    String insertContents = "";
                    for(IStatement s: this.statements.subList(1, this.statements.size()))
                    {
                        insertContents += "\n" + s.toString();
                    }
                    UpdateBlockStatement insertBlock = new UpdateBlockStatement(BLOCKTYPE.INSERT, insertContents, this.getGraph());
                    IStatement deleteBlock = this.statements.get(0);
                    this.statements.clear();
                    this.statements.add(deleteBlock);
                    this.statements.add(insertBlock);
                }

                if(this.getUnknowns().size() > 0)
                {
                    if(!iterator.peekNext().toLowerCase().startsWith("where") )
                    {
                        throw new InvalidSPARQLException("Invalid SPARQL on line: " + iterator.getCurrentLine() + " expected WHERE clause after " + this.type.name() + " clause around " + iterator.getPrevious());
                    }
                }


                if(iterator.hasNext() && iterator.peekNext().toLowerCase().startsWith("where"))
                {
                    String where = iterator.next();
                    iterator.breakOff(where.substring(0, 5));
                    this.whereBlock = new WhereBlockStatement(iterator);
                }
                return;
            }

            // hooray we have a new inner block!
            if(iterator.peekNext().startsWith("{") || iterator.peekNext().toLowerCase().startsWith("graph")) {
                if(!block.trim().isEmpty()) {
                    statements.add(new SimpleStatement(block));
                }
                block = "";
                statements.add(new ParenthesesBlock(iterator));
                continue;
            }

            String nextPart = iterator.nextIncludingNewLines();

            if (nextPart.trim().equals("\n")) {
                block += nextPart;
            } else {
                block += " " + nextPart;
            }
        }
    }

    public String toString()
    {
        String toReturn = "";
        if(type == BLOCKTYPE.DELETE_INSERT)
        {
            toReturn += this.statements.get(0);
            toReturn += this.statements.get(1);
        }
        else {
            if (type == BLOCKTYPE.INSERT) {
                toReturn += "INSERT \n{";
            } else {
                toReturn += "DELETE \n{";
            }

            for (IStatement statement : statements) {
                toReturn += statement.toString() + "\n";
            }
            toReturn = toReturn.substring(0, toReturn.length());

            toReturn += "\n}";
        }

        if(this.whereBlock != null)
        {
            toReturn += "\n" + this.whereBlock.toString();
        }

        return toReturn;
    }

    public WhereBlockStatement getWhereBlock()
    {
        return this.whereBlock;
    }

    public StatementType getType()
    {
        return StatementType.UPDATEBLOCK;
    }

    public BLOCKTYPE getUpdateType() { return this.type; }

    public void setUpdateType(BLOCKTYPE type){this.type = type;}

    public void setWhereBlock(WhereBlockStatement whereBlock)
    {
        this.whereBlock = whereBlock;
    }

    public UpdateBlockStatement clone()
    {
        UpdateBlockStatement clone = new UpdateBlockStatement();

        clone.setGraph(this.graph);
        if(this.whereBlock != null)
            clone.setWhereBlock(this.whereBlock.clone());

        clone.setUpdateType(this.type);

        for(IStatement s : this.statements)
            clone.getStatements().add(s.clone());

        for(String s : this.getUnknowns())
            clone.getUnknowns().add(s);

        return clone;
    }

    public String getGraph()
    {
        if(this.graph.isEmpty())
        {
            String graph = "";
            for(IStatement statement : this.statements)
            {
                if(statement.getType() == StatementType.PARENTHESESBLOCK)
                {
                    if(!statement.getGraph().isEmpty())
                    {
                        return statement.getGraph();
                    }
                }
            }
        }
        return this.graph;
    }


//    private void extractBlocks(SplitQuery.SplitQueryIterator iterator, UpdateBlockStatement.BLOCKTYPE type) throws InvalidSPARQLException {
//        // a where block is normally homogenous
//        if(type == UpdateBlockStatement.BLOCKTYPE.WHERE)
//        {
//            statements.add(new UpdateBlockStatement(type, parseBlock(iterator), null));
//            return;
//        }
//
//        // but update blocks can contain more than one graph, if there are then for each sub block we will make a new block
//        // in the blocks list
//        if(iterator.peekNext().toLowerCase().startsWith("graph")) {
//            while (!iterator.peekNext().startsWith("}")) {
//                String graphToken = iterator.next();
//                if (!graphToken.toLowerCase().startsWith("graph")) {
//                    throw new InvalidSPARQLException("Invalid SPARQL on line: " + iterator.getCurrentLine() + " was expecting 'graph' near: " + iterator.getPrevious());
//                }
//                if (!graphToken.toLowerCase().equals("graph")) {
//                    String[] toInsert = {graphToken.substring(0, 5), graphToken.substring(5, graphToken.length()).trim()};
//                    iterator.replaceCurrent(toInsert);
//                }
//                String graph = iterator.next();
//                if (!(graph.startsWith("<") && graph.endsWith(">"))) {
//                    throw new InvalidSPARQLException("Invalid SPARQL on line: " + iterator.getCurrentLine() + " " + graph + " not a valid graphname");
//                }
//                graph = graph.substring(1, graph.length() - 1);
//                blocks.add(new UpdateBlockStatement(type, parseBlock(iterator), graph));
//            }
//        }
//        else
//        {
//            blocks.add(new UpdateBlockStatement(type, parseBlock(iterator), null));
//        }
//    }

}

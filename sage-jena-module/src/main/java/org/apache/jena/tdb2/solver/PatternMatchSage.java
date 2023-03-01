package org.apache.jena.tdb2.solver;
// In this package so it can access SolverLibTDB package functions.

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.Abortable;
import org.apache.jena.sparql.engine.iterator.QueryIterAbortable;
import org.apache.jena.sparql.engine.main.solver.SolverLib;
import org.apache.jena.sparql.engine.main.solver.SolverRX4;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.arq.VolcanoIterator;
import fr.gdd.sage.arq.VolcanoIteratorFactory;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.SerializableRecord;



/**
 * Utility class dedicated to the pattern matching with Sage over TDB2.
 *
 * Comes from :
 * <https://github.com/apache/jena/blob/main/jena-tdb2/src/main/java/org/apache/jena/tdb2/solver/PatternMatchTDB2.java>
 */
public class PatternMatchSage {

    /**
     * Creates the builder for triple patterns.
     */
    public static QueryIterator matchTriplePattern(BasicPattern pattern, QueryIterator input, ExecutionContext context) {
        return match(pattern, input, context);
    }

    /**
     * Creates the build for quad patterns.
     */
    public static QueryIterator matchQuadPattern(BasicPattern pattern, Node graphNode, QueryIterator input, ExecutionContext context) {
        return match(pattern, input, context, graphNode);
    }

    /**
     * Used by triple and quad pattern to create the builder of scans.
     */
    static QueryIterator match(BasicPattern pattern, QueryIterator input, ExecutionContext context, Node... graphNode) {
        if (graphNode.length > 0) {
            if ( Quad.isUnionGraph(graphNode[0]))
                graphNode[0] = Node.ANY;
            if ( Quad.isDefaultGraph(graphNode[0]))
                graphNode = null;
        }
        boolean anyGraph = (graphNode == null ? false : (Node.ANY.equals(graphNode)));
        var graph = graphNode == null ? null : graphNode[0];
        
        SageInput<?> sageInput = context.getContext().get(SageConstants.input);
        long deadline = sageInput.getDeadline();
        SageOutput<?> sageOutput = context.getContext().get(SageConstants.output);

        DatasetGraphTDB activeGraph = TDBInternal.getDatasetGraphTDB(context.getDataset());
        NodeTupleTable nodeTupleTable = (graph == null) ?
            activeGraph.getTripleTable().getNodeTupleTable() : 
            activeGraph.getQuadTable().getNodeTupleTable();
        
        Predicate<Tuple<NodeId>> filter = QC2.getFilter(context.getContext());
        
        var conv = SolverLibTDB.convFromBinding(nodeTupleTable.getNodeTable());
        Iterator<BindingNodeId> chain = Iter.map(input, conv);

        List<Abortable> killList = new ArrayList<>();
        int numberOfScans = 0;
        for (Triple triple: pattern.getList()) {
            // create the function that will be called everytime a
            // scan iterator is created.
            final int scanId = numberOfScans;
            Function<BindingNodeId, Iterator<BindingNodeId>> step =
                bnid -> find(bnid, nodeTupleTable, graph, triple, anyGraph, filter, context,
                             scanId, deadline, sageInput, sageOutput);
            
            chain = Iter.flatMap(chain, step);
            chain = SolverLib.makeAbortable(chain, killList);
            numberOfScans += 1;
        }
        
        Iterator<Binding> iterBinding = SolverLibTDB.convertToNodes(chain, nodeTupleTable.getNodeTable());
        QueryIterAbortable abortable = new QueryIterAbortable(iterBinding, killList, input, context);
        return abortable;
    }


    // This function is called every time an iterator is created but
    // the `bnid` changes and contains the bindings created so far
    // that may be injected in the current iterator.
    // 
    // from <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb1/src/main/java/org/apache/jena/tdb/solver/SolverRX.java#L78>
    private static Iterator<BindingNodeId> find(BindingNodeId bnid, NodeTupleTable nodeTupleTable,
                                                Node xGraphNode, Triple xPattern,
                                                boolean anyGraph,
                                                Predicate<Tuple<NodeId>> filter,
                                                ExecutionContext context,
                                                int id, long deadline,
                                                SageInput<?> sageInput, SageOutput<?> sageOutput) {
        // (TODO) add filter when on NodeId
        // System.out.printf("filter %s \n", filter.toString());
        
        NodeTable nodeTable = nodeTupleTable.getNodeTable();
        Binding input = bnid.isEmpty() ? BindingFactory.empty() : new BindingTDB(bnid, nodeTable);
        Triple tPattern = Substitute.substitute(xPattern, input);
        Node graphNode = Substitute.substitute(xGraphNode, input);

        Node tGraphNode = anyGraph ? Quad.unionGraph : graphNode ;
        // graphNode is ANY for union graph and null for default graph.
        // Var to ANY, Triple Term to ANY.

        Node g = ( graphNode == null ) ? null : SolverLib.nodeTopLevel(graphNode);
        Node s = SolverLib.nodeTopLevel(tPattern.getSubject());
        Node p = SolverLib.nodeTopLevel(tPattern.getPredicate());
        Node o = SolverLib.nodeTopLevel(tPattern.getObject());
        Tuple<Node> patternTuple = ( g == null )
                ? TupleFactory.create3(s, p, o)
                : TupleFactory.create4(g, s, p, o);
        Tuple<NodeId> patternTupleId = TupleLib.tupleNodeIds(nodeTable, patternTuple);

        Map<Integer, VolcanoIterator> iterators = context.getContext().get(SageConstants.iterators);
        
        VolcanoIteratorFactory factory = context.getContext().get(SageConstants.scanFactory);        
        VolcanoIterator volcanoIterator = factory.getScan(patternTupleId, id);
        
        if (!iterators.containsKey(id)) {
            if (sageInput != null && sageInput.getState() != null && sageInput.getState().containsKey(id)) {
                volcanoIterator.skip((SerializableRecord) sageInput.getState(id));
            }
        }
        iterators.put(id, volcanoIterator); // register and/or erase previous iterator

        Iterator<Binding> matched = Iter.iter(volcanoIterator)
            .map(dQuad->SolverRX4.matchQuad(input, dQuad, tGraphNode, tPattern))
            .removeNulls();
        return SolverLibTDB.convFromBinding(matched, nodeTable);
    }
}


package fr.gdd.sage.arq;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.Abortable;
import org.apache.jena.sparql.engine.iterator.QueryIterAbortable;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.main.solver.SolverLib;
import org.apache.jena.sparql.engine.main.solver.SolverRX4;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.tdb2.solver.BindingNodeId;
import org.apache.jena.tdb2.solver.BindingTDB;
import org.apache.jena.tdb2.solver.QC2;
import org.apache.jena.tdb2.solver.SolverLibTDB;
import org.apache.jena.tdb2.solver.SolverRX;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.GraphViewSwitchable;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;

import fr.gdd.sage.ReflectionUtils;
import fr.gdd.sage.interfaces.SageInput;
import fr.gdd.sage.interfaces.SageOutput;
import fr.gdd.sage.jena.JenaBackend;



/**
 * Class in charge of creating preemptable basic graph patterns
 * (BGPs).
 */
public class SageStageGenerator implements StageGenerator {
    StageGenerator parent;
    
    public SageStageGenerator(StageGenerator parent) {
        this.parent = parent;
    }
    
    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        if (!(execCxt.getActiveGraph() instanceof GraphViewSwitchable)) {
            return parent.execute(pattern, input, execCxt);
        }
        
        return executeTriplePattern(pattern, input, execCxt);
    }


    /**
     * The actual `execute()` standalone function that is meant to be
     * called from other classes when need be.
     */
    public static QueryIterator executeTriplePattern(BasicPattern pattern,
                                                     QueryIterator input,
                                                     ExecutionContext execCxt) {
        SageInput sageInput = execCxt.getContext().get(SageConstants.input);
        JenaBackend backend = (JenaBackend) sageInput.getBackend();
        
        Method convFromBindingMethod = ReflectionUtils._getMethod(SolverLibTDB.class,
                                                                  "convFromBinding",
                                                                  NodeTable.class);
        var conv = (Function<Binding, BindingNodeId>) ReflectionUtils._callMethod(convFromBindingMethod,
                                                                                  null, null, backend.getNodeTable());
        Iterator<BindingNodeId> chain = Iter.map(input, conv);
        List<Abortable> killList = new ArrayList<>();

        int sumId = 0;
        for (Triple triple: pattern.getList()) {
            // From <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb1/src/main/java/org/apache/jena/tdb/solver/SolverRX.java#L73>
            // anygraph false => null , ie. search default graph for triples
            Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext());
            Integer scanId = new Integer(sumId);
            // create the function that will be called everytime a
            // scan iterator is created.
            Function<BindingNodeId, Iterator<BindingNodeId>> step =
                bnid -> find(bnid, backend.getNodeTripleTupleTable(), null, triple, false, filter, execCxt, scanId);
            
            chain = Iter.flatMap(chain, step);
            chain = SolverLib.makeAbortable(chain, killList);
            sumId += 1;
        }

        //  Iterator<Binding> iterBinding = SolverLibTDB.convertToNodes(chain, nodeTable);
        Method convertToNodesMethod = ReflectionUtils._getMethod(SolverLibTDB.class,
                                                                 "convertToNodes",
                                                                 Iterator.class,
                                                                 NodeTable.class);
        var iterBinding = (Iterator<Binding>) ReflectionUtils._callMethod(convertToNodesMethod,
                                                                          null, null, chain, backend.getNodeTable());

        QueryIterAbortable abortable = new QueryIterAbortable(iterBinding, killList, input, execCxt);
        return abortable;
    }
    


    // This function is called every time an iterator is created but
    // the `bnid` changes and contains the bindings created so far
    // that may be injected in the current iterator.
    // 
    // from <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb1/src/main/java/org/apache/jena/tdb/solver/SolverRX.java#L78>
    private static Iterator<BindingNodeId> find(BindingNodeId bnid, NodeTupleTable nodeTupleTable,
                                                Node xGraphNode, Triple xPattern,
                                                boolean anyGraph, Predicate<Tuple<NodeId>> filter,
                                                ExecutionContext execCxt, Integer id) {
        // (TODO) maybe add preempt metadata in execution context.
        // for (Symbol s : execCxt.getContext().keys()) {
        // System.out.printf("Symbol %s =>> %s \n" ,s.toString() , execCxt.getContext().getAsString(s));
        // }
        
        System.out.printf("CREATE SCAN n°%s bnid %s\n", id, bnid.toString());
        
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
                ? TupleFactory.create3(s,p,o)
                : TupleFactory.create4(g,s,p,o);


        // (TODO) replasce dsgIter by our preemptable iterator.
        // Iterator<Quad> dsgIter = SolverRX.accessData(patternTuple, nodeTupleTable, anyGraph, filter, execCxt);
        // Method accessDataMethod = ReflectionUtils._getMethod(SolverRX.class, "accessData",
        //                                                      Tuple.class, NodeTupleTable.class,
        //                                                      boolean.class, Predicate.class,
        //                                                      ExecutionContext.class);
        // Iterator<Quad> dsgIter = (Iterator<Quad>) ReflectionUtils._callMethod(accessDataMethod, null, null,
        //                                                                       patternTuple, nodeTupleTable, anyGraph, filter, execCxt);

        long deadline = execCxt.getContext().get(SageConstants.deadline);
        SageOutput output = execCxt.getContext().get(SageConstants.output);


        Map<Integer, VolcanoIterator> iterators = execCxt.getContext().get(SageConstants.iterators);
        SageInput sageInput = execCxt.getContext().get(SageConstants.input);
        JenaBackend backend = (JenaBackend) sageInput.getBackend();
        
        VolcanoIterator volcanoIterator =  new VolcanoIterator(backend.search(nodeTable.getNodeIdForNode(s),
                                                                              nodeTable.getNodeIdForNode(p),
                                                                              nodeTable.getNodeIdForNode(o)),
                                                               // (TODO) Graph
                                                               backend.getNodeTable(),
                                                               deadline,
                                                               iterators,
                                                               output,
                                                               id);
        if (!iterators.containsKey(id)) {
            if (sageInput != null && sageInput.getState() != null) {
                volcanoIterator.skip((Record) sageInput.getState(id));
            }
        }
        iterators.put(id, volcanoIterator); // register and/or erase previous iterator
        Iterator<Quad> dsgIter = (Iterator<Quad>) volcanoIterator;
        
        Iterator<Binding> matched = Iter.iter(dsgIter)
            .map(dQuad->SolverRX4.matchQuad(input, dQuad, tGraphNode, tPattern)).removeNulls();
        
        //  convFromBinding(matched, nodeTable);
        // => Iter.map(matched, SolverLibTDB.convFromBinding(nodeTable));
        Method convFromBindingMethod = ReflectionUtils._getMethod(SolverLibTDB.class,
                                                                  "convFromBinding",
                                                                  NodeTable.class);
        var conv = (Function<Binding, BindingNodeId>) ReflectionUtils._callMethod(convFromBindingMethod,
                                                                                  null, null,
                                                                                  nodeTable);
        return Iter.map(matched, conv);
    }

    
}

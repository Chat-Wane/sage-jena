package fr.gdd;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Graph;
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
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.main.solver.SolverLib;
import org.apache.jena.sparql.engine.main.solver.SolverRX4;
import org.apache.jena.tdb2.solver.BindingNodeId;
import org.apache.jena.tdb2.solver.BindingTDB;
import org.apache.jena.tdb2.solver.SolverLibTDB;
import org.apache.jena.tdb2.solver.SolverRX;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.GraphViewSwitchable;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;

import fr.gdd.common.ReflectionUtils;
import fr.gdd.jena.JenaBackend;



public class SageStageGenerator implements StageGenerator {
    StageGenerator parent;

    JenaBackend backend;

    public SageStageGenerator(StageGenerator parent, JenaBackend backend) {
        this.parent = parent;
        this.backend = backend;
    }

    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        if (!(execCxt.getActiveGraph() instanceof GraphViewSwitchable)) {
            return parent.execute(pattern, input, execCxt);
        }

        Method convFromBindingMethod = ReflectionUtils._getMethod(SolverLibTDB.class,
                                                                  "convFromBinding",
                                                                  NodeTable.class);
        var conv = (Function<Binding, BindingNodeId>) ReflectionUtils._callMethod(convFromBindingMethod,
                                                                                  null, null, backend.node_table);
        Iterator<BindingNodeId> chain = Iter.map(input, conv);
        List<Abortable> killList = new ArrayList<>();
        
        for (Triple triple: pattern.getList()) {
            System.out.printf("%s\n", triple.toString());
            
            Tuple<Node> patternTuple = TupleFactory.create3(triple.getSubject(),
                                                            triple.getPredicate(),
                                                            triple.getObject());


            // From <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb1/src/main/java/org/apache/jena/tdb/solver/SolverRX.java#L73>
            Function<BindingNodeId, Iterator<BindingNodeId>> step =
                bnid -> find(bnid, backend.node_triple_tuple_table, null, triple, true, null, execCxt);
            chain = Iter.flatMap(chain, step);
            System.out.printf("%s\n", patternTuple.toString());
            
            
        }


        //  Iterator<Binding> iterBinding = SolverLibTDB.convertToNodes(chain, nodeTable);
        Method convertToNodesMethod = ReflectionUtils._getMethod(SolverLibTDB.class,
                                                                 "convertToNodes",
                                                                 Iterator.class,
                                                                 NodeTable.class);
        var iterBinding = (Iterator<Binding>) ReflectionUtils._callMethod(convertToNodesMethod,
                                                                          null, null, chain, backend.node_table);
        
        System.out.println("CARJACKED");

        QueryIterAbortable abortable = new QueryIterAbortable(iterBinding, killList, input, execCxt);
        return abortable;
    }




    
    // from <https://github.com/apache/jena/blob/ebc10c4131726e25f6ffd398b9d7a0708aac8066/jena-tdb1/src/main/java/org/apache/jena/tdb/solver/SolverRX.java#L78>
    private static Iterator<BindingNodeId> find(BindingNodeId bnid, NodeTupleTable nodeTupleTable,
                                                Node xGraphNode, Triple xPattern,
                                                boolean anyGraph, Predicate<Tuple<NodeId>> filter,
                                                ExecutionContext execCxt) {
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


        System.out.printf("MEOW %s\n", patternTuple);
        // Iterator<Quad> dsgIter = SolverRX.accessData(patternTuple, nodeTupleTable, anyGraph, filter, execCxt);
        Method accessDataMethod = ReflectionUtils._getMethod(SolverRX.class, "accessData",
                                                             Tuple.class, NodeTupleTable.class,
                                                             boolean.class, Predicate.class,
                                                             ExecutionContext.class);
        Iterator<Quad> dsgIter = (Iterator<Quad>) ReflectionUtils._callMethod(accessDataMethod, null, null,
                                                                              patternTuple, nodeTupleTable, anyGraph, filter, execCxt);


        Iterator<Binding> matched = Iter.iter(dsgIter)
            .map(dQuad->SolverRX4.matchQuad(input, dQuad, tGraphNode, tPattern)).removeNulls();


        //  convFromBinding(matched, nodeTable);
        // => Iter.map(matched, SolverLibTDB.convFromBinding(nodeTable));
        Method convFromBindingMethod = ReflectionUtils._getMethod(SolverLibTDB.class,
                                                                  "convFromBinding",
                                                                  NodeTable.class);
        var conv = (Function<Binding, BindingNodeId>) ReflectionUtils._callMethod(convFromBindingMethod, null, null,
                                                                                  nodeTable);
        var meow = Iter.map(matched, conv);

        return meow;
    }

    
}

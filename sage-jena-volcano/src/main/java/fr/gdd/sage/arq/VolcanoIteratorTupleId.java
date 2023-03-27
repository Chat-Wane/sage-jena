package fr.gdd.sage.arq;

import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.trans.bplustree.PreemptJenaIterator;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A Volcano Iterator that works on {@link Tuple<NodeId>} instead of
 * {@link  org.apache.jena.sparql.core.Quad}. They are supposedly more efficient.
 */
public class VolcanoIteratorTupleId implements Iterator<Tuple<NodeId>> {

    public BackendIterator<NodeId, SerializableRecord> wrapped;
    NodeTable nodeTable;

    SageInput<?>  input;
    SageOutput<?> output;
    int id;

    // Cannot pause at first execution of the `hasNext()`.
    boolean first = false;


    public VolcanoIteratorTupleId(BackendIterator<NodeId, SerializableRecord> wrapped, NodeTable nodeTable,
                                  SageInput<?> input, SageOutput<?> output, Integer id) {
        this.wrapped = wrapped;
        this.nodeTable = nodeTable;
        this.input = input;
        this.output = output;
        this.id = id;
    }

    @Override
    public boolean hasNext() {
        if (!first && (System.currentTimeMillis() >= input.getDeadline() || output.size() >= input.getLimit())) {
            // (TODO) cleaner
            if (this.output.getState().containsKey(id)) {
                return false;
            }
            boolean shouldSaveCurrent = Objects.isNull(this.output.getState()) ||
                    this.output.getState().keySet().stream().filter(k -> k < 1000)
                            .collect(Collectors.toUnmodifiableList()).isEmpty();

            Pair toSave = shouldSaveCurrent ?
                    new Pair(id, this.wrapped.current()):
                    new Pair(id, this.wrapped.previous());
            this.output.addState(toSave);

            return false;
        }
        first = false;

        return wrapped.hasNext();
    }

    @Override
    public Tuple<NodeId> next() {
        wrapped.next();
        return ((PreemptJenaIterator) wrapped).getCurrentTuple();
    }

    public void skip(SerializableRecord to) {
        first = true; // skip so first `hasNext` is mandatory
        wrapped.skip(to);
    }
}

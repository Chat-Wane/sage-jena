package fr.gdd.sage.sager;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * An attempt at a better version of BindingTDB… Issue with BindingTDB is
 * the `TupleTable` that needs to be the same everywhere… without possibility to duplicate
 * parent's one easily.
 */
public class BindingId2Value implements Iterable<Var>, Binding {



    public static class IdValueTable {
        NodeId id = null;
        Node value = null;
        final NodeTable table;

        public IdValueTable() {this.table = null; }
        public IdValueTable(final NodeTable table) { this.table = table; }

        public IdValueTable setId(NodeId id){
            this.id = id;
            return this;
        }

        public IdValueTable setValue(Node value) {
            this.value = value;
            return this;
        }

        public Node getValue() { return Objects.isNull(value) ? table.getNodeForNodeId(id) : value; }
        public NodeId getId() { return Objects.isNull(id) ? table.getNodeIdForNode(value) : id; }
    }

    /* ************************************************************************** */

    final Map<Var, IdValueTable> var2all = new HashMap<>();
    BindingId2Value parent;
    NodeTable defaultTable = null;

    public BindingId2Value () { parent = null; } // ROOT

    public BindingId2Value setParent(BindingId2Value parent) { // avoid cloning parent
        this.parent = parent;
        return this;
    }

    public BindingId2Value setDefaultTable(NodeTable table) {
        this.defaultTable = table;
        return this;
    }

    public BindingId2Value put(Var var, NodeId id) {
        return put(var, id, defaultTable);
    }

    public BindingId2Value put(Var var, NodeId id, NodeTable table) {
        var2all.put(var, new IdValueTable(table).setId(id));
        return this;
    }

    public BindingId2Value put(Var var, Node value) {
        var2all.put(var, new IdValueTable(getDefaultTable()).setValue(value));
        return this;
    }

    public NodeId getId(Var var) {
        IdValueTable found = var2all.getOrDefault(var, null);
        if (Objects.isNull(found)) {
            return Objects.isNull(parent) ? null : parent.getId(var);
        }
        return found.getId();
    }

    public Node getValue(Var var) {
        IdValueTable found = var2all.getOrDefault(var, null);
        if (Objects.isNull(found)) {
            return Objects.isNull(parent) ? null : parent.getValue(var);
        }
        return found.getValue();
    }

    @Override
    public Iterator<Var> iterator() {
        return (Objects.isNull(this.parent)) ? this.var2all.keySet().iterator(): this.getVars().iterator();
    }

    private Set<Var> getVars() {
        if (Objects.isNull(this.parent)) {
            return this.var2all.keySet();
        } else {
            Set<Var> keys = new HashSet<>(this.var2all.keySet());
            keys.addAll(parent.getVars());
            return keys;
        }
    }

    public NodeTable getDefaultTable() {
        if (Objects.nonNull(defaultTable)) {
            return defaultTable;
        } else if (Objects.nonNull(parent)){
            return parent.getDefaultTable();
        } else {
            return null;
        }
    }


    @Override
    public Iterator<Var> vars() {
        return iterator();
    }

    @Override
    public void forEach(BiConsumer<Var, Node> action) {
        throw new UnsupportedOperationException("forEach");
    }

    @Override
    public boolean contains(Var var) {
        if (this.var2all.containsKey(var)) {
            return true;
        }
        if (Objects.nonNull(parent)) {
            return parent.contains(var);
        }
        return false;
    }

    @Override
    public Node get(Var var) {
        return getValue(var);
    }

    @Override
    public int size() {
        return getVars().size();
    }

    @Override
    public boolean isEmpty() {
        if (!this.var2all.isEmpty()) {
            return false;
        }
        if (Objects.nonNull(parent)){
            return parent.isEmpty();
        }
        return false;
    }

}

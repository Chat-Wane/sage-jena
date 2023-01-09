package fr.gdd.jena;

import fr.gdd.common.ReflectionUtils;

import static java.lang.String.format;
import java.lang.reflect.Field;
import java.util.Iterator;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.tdb2.TDBException;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.tupletable.TupleIndex;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;
import org.apache.jena.tdb2.store.tupletable.TupleTable;



public class PreemptableTupleTable extends TupleTable {

    // by default, we get
    // SPO POS OSP
    // GSPO GPOS GOSP
    // POSG OSPG SPOG
    public PreemptableTupleTable(TupleTable parent) {
        super(parent.getTupleLen(), parent.getIndexes());
    }

    // <https://github.com/apache/jena/blob/c4e999d633b2532b504b35937db4bec9a7c2e539/jena-tdb2/src/main/java/org/apache/jena/tdb2/store/tupletable/TupleTable.java#L133>
    private TupleIndex findIndex(Tuple<NodeId> pattern) {
        if ( super.getTupleLen() != pattern.len() )
            throw new TDBException(format("Mismatch: finding tuple of length %d in a table of tuples of length %d", pattern.len(), super.getTupleLen()));
        
        int numSlots = 0;
        // Canonical form.
        for ( int i = 0; i < super.getTupleLen() ; i++ ) {
            NodeId x = pattern.get(i);
            if ( ! NodeId.isAny(x) )
                numSlots++;
            if ( NodeId.isDoesNotExist(x)) {
                // System.out.println("WOOF");
                return null;
            }
        }
        
        if (numSlots == 0) {
            Field scanAllIndexField = ReflectionUtils._getField(TupleTable.class, "scanAllIndex");
            TupleIndex ti = (TupleIndex) ReflectionUtils._callField(scanAllIndexField, TupleTable.class, this);
            return ti;
        }
        
        int indexNumSlots = 0;
        TupleIndex index = null;
        for ( TupleIndex idx : super.getIndexes() ) {
            if ( idx != null ) {
                int w = idx.weight( pattern );
                if ( w > indexNumSlots ) {
                    indexNumSlots = w;
                    index = idx;
                }
            }
        }

        if ( index == null )
            // No index at all.  Scan.
            index = super.getIndexes()[0];
        
        return index;
    }

    public JenaIterator preemptable_find(Tuple<NodeId> pattern) {
        TupleIndexRecord tir = (TupleIndexRecord) this.findIndex(pattern);
        PreemptableTupleIndexRecord ptir = new PreemptableTupleIndexRecord(tir);
        return ptir.scan(pattern);
    }
    
}

package fr.gdd;

import java.lang.reflect.Field;

import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;



public class PreemptableTupleIndexRecord {

    RecordFactory factory;
    
    PreemptableTupleIndexRecord(TupleIndexRecord tir) {
        Field factoryField = ReflectionUtils._getField(TupleIndexRecord.class, "factory");
        this.factory = (RecordFactory) ReflectionUtils._callField(factoryField, tir.getClass(), tir);
    }

    

}

package org.apache.jena.dboe.trans.bplustree;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.VCARD;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JenaIteratorTest {

    static Dataset dataset = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        Model model = ModelFactory.createDefaultModel();
        model.read("<http://db.uwaterloo.ca/~galuc/wsdbm/City0> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>");
/*<http://db.uwaterloo.ca/~galuc/wsdbm/City100>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country2> .
<http://db.uwaterloo.ca/~galuc/wsdbm/City101>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country2> .
<http://db.uwaterloo.ca/~galuc/wsdbm/City102>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country17> .
<http://db.uwaterloo.ca/~galuc/wsdbm/City103>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country3> .
<http://db.uwaterloo.ca/~galuc/wsdbm/City104>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country1> .
<http://db.uwaterloo.ca/~galuc/wsdbm/City105>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country0> .
<http://db.uwaterloo.ca/~galuc/wsdbm/City106>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country10> .
<http://db.uwaterloo.ca/~galuc/wsdbm/City107>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country23> .
<http://db.uwaterloo.ca/~galuc/wsdbm/City108>	<http://www.geonames.org/ontology#parentCountry>	<http://db.uwaterloo.ca/~galuc/wsdbm/Country1> ."*/

        dataset.setDefaultModel(model);
    }


    


}
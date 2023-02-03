package fr.gdd.volcano;

import org.apache.jena.sparql.util.Symbol;



public class SageConstants {

    public static final String systemVarNS = "http://sage.gdd.fr/Sage#";

    public static final String sageSymbolPrefix = "sage";

    

    public static Symbol allocSymbol(String name) {
        return Symbol.create(systemVarNS + name);
    }
}

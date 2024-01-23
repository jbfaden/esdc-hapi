
package org.esdc.hapi;

/**
 * These are utilities which should probably be in SourceUtil of HapiServerBase.
 * @author jbf
 */
public class Util {
        
    /**
     * parse the string to an integer array.
     * "[1,2,3]" -> new int[] { 1,2,3 }
     * The string may or may not contain brackets.
     * @param size
     * @return int array
     */
    public static int[] parseIntArray(String size) {
        if ( size.startsWith("[") && size.endsWith("]") ) {
            size= size.substring(1,size.length()-1);
        }
        String[] ss= size.split(",",-2);
        int[] result= new int[ss.length];
        for ( int i=0; i<ss.length; i++ ) {
            result[i]= Integer.parseInt(ss[i]);
        }
        return result;
    }    
    
}

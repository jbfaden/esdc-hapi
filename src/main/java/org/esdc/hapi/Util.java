
package org.esdc.hapi;

/**
 * These are utilities which should probably be in SourceUtil of HapiServerBase,
 * but for now are available to this code.
 * @author jbf
 */
public class Util {
        
    /**
     * parse the string to an integer array.
     * "[1,2,3]" -> new int[] { 1,2,3 }
     * The string may or may not contain brackets.
     * @param size string like "[1,2,3]"
     * @return int array like <code>new int[] { 1,2,3 }</code>
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

    /**
     * return version tag to ensure that new version is seen by the server
     */
    public static String getVersion() {
        return "20240227.1";
    }
    
}

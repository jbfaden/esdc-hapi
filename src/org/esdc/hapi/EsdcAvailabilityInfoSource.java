
package org.esdc.hapi;

/**
 *
 * @author jbf
 */
public class EsdcAvailabilityInfoSource {
        
    public static String getInfo( String id ) {
        String uri= "SELECT+begin_time,end_time,filepath,filename+FROM+"+id+"+WHERE+instrument='MAG'+AND+level='L2'";
        return null;
    }
}

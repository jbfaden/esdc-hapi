
package org.hapiserver;

import java.io.IOException;
import java.io.OutputStream;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.HapiRecord;

/**
 * A DataFormatter converts a HAPI record in to a formatted record on the stream.
 * @author jbf
 */
public interface DataFormatter {
        
    /**
     * configure the format.
     * @param info JSON info describing the records.
     * @param out the output stream
     * @param record a single HAPI record
     * @throws java.io.IOException
     */
    public void initialize( JSONObject info, OutputStream out, HapiRecord record) throws IOException;
    
    /**
     * send the record to the output stream
     * @param out the output stream
     * @param record the HAPI record
     * @throws IOException 
     */
    public void sendRecord( OutputStream out, HapiRecord record ) throws IOException;
    
    /**
     * perform any final operations to the stream.  Clients should not close the stream!
     * @param out the output stream
     * @throws java.io.IOException 
     */
    public void finalize( OutputStream out )  throws IOException;
    
}

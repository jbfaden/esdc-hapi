
package org.hapiserver;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Format to binary types.  Note that TransferTypes use doubles to communicate,
 * so floating point numbers may not format precisely.
 * @author jbf
 */
public class BinaryDataFormatter implements DataFormatter {
  
    private static final Logger logger= Logger.getLogger("hapi");    
    
    private static final Charset CHARSET= Charset.forName("UTF-8");
    
    private static interface TransferType {
        public void write( HapiRecord record, int i, ByteBuffer buffer);
        public int sizeBytes();        
    }
    
    TransferType[] transferTypes;
    double[] fill;
    ByteBuffer b;
    int bufferSize;
    int sentRecordCount;
    
    public BinaryDataFormatter( ) {
    }
        
    
    /**
     * properly trim the byte array containing a UTF-8 String to a limit
     * @param bytes the bytes
     * @param k the number of bytes
     * @return the bytes properly trimmed.
     */
    public static byte[] trimUTF8( byte[] bytes, int k ) {
        if ( bytes.length==k ) return bytes;
        int b= bytes[k];
        if ( b>127 ) { // uh-oh, we are mid-UTF8-extended character.
            while ( k>0 && b>127 ) {
                k=k-1;
                b= bytes[k];
            }
        }
        bytes= Arrays.copyOf( bytes, k );
        return bytes;
    }
    
    /**
     * return the fill value or NaN if a fill value is not used.
     * @param parameter the JSONObject for the parameter, containing "name" and "units"
     * @return the fill value or NaN if a fill value is not used.
     */
    private double getFill( JSONObject parameter ) {
        String sfill= parameter.optString("fill","NaN");
        if ( sfill==null ) sfill= "NaN";
        return Double.parseDouble( sfill );        
    }
        
    @Override
    public void initialize( JSONObject info, OutputStream out, HapiRecord record) {
        try {
            transferTypes= new TransferType[record.length()];
            fill= new double[record.length()];
            
            bufferSize= 0;
            
            int totalFields= 0;
            JSONArray parameters= info.getJSONArray("parameters");
            for ( int i=0; i<parameters.length(); i++ ) {
                JSONObject parameter= parameters.getJSONObject(i);
                TransferType tt;
                final String stype = parameter.getString("type");
                double fl=-1;

                int nfields;
                if ( parameter.has("size") ) {
                    JSONArray ja= (JSONArray)parameter.get("size");
                    int prod= 1;
                    for ( int j=0; j<ja.length(); j++ ) {
                        prod*= ja.getInt(j);
                    }
                    nfields= prod;
                } else {
                    nfields= 1;
                }
                
                switch (stype) {
                    case "isotime":
                        {
                            if ( !parameter.has("length") ) throw new RuntimeException("required tag length is missing");
                            final int len= parameter.getInt("length");
                            tt= new TransferType() {
                                @Override
                                public void write( HapiRecord record, int i, ByteBuffer buffer) {
                                    if ( nfields==1 ) {
                                        buffer.put( record.getIsoTime(i).getBytes(CHARSET) );
                                    } else {
                                        String[] ss= record.getIsoTimeArray(i);
                                        for ( int j=0; j<nfields; j++ ) {
                                            buffer.put( ss[j].getBytes(CHARSET) );
                                        }
                                    }
                                    
                                }
                                @Override
                                public int sizeBytes() {
                                    return len;
                                }
                            };      
                            break;
                        }
                    case "string":
                        {
                            if ( !parameter.has("length") ) {
                                throw new RuntimeException("required tag length is missing");
                            } 
                            final int len= parameter.getInt("length");
                            final byte[] zeros= new byte[len];
                            for ( int i2=0; i2<zeros.length; i2++ ) zeros[i2]= 0;
                            tt= new TransferType() {
                                @Override
                                public void write( HapiRecord record, int i, ByteBuffer buffer) {
                                    if ( nfields>1 ) throw new IllegalArgumentException("not supported, email jbfaden"); //TODO: nfields
                                    byte[] bytes= record.getString(i).getBytes( Charset.forName("UTF-8") );
                                    if ( bytes.length==len ) {
                                        buffer.put( bytes );
                                    } else if ( bytes.length<len ) {
                                        buffer.put( bytes, 0, bytes.length );
                                        buffer.put( zeros, bytes.length, len-bytes.length );
                                    } else {
                                        bytes= trimUTF8( bytes, len );
                                        buffer.put( bytes, 0, bytes.length );
                                        buffer.put( zeros, bytes.length, len-bytes.length );
                                    }
                                }
                                @Override
                                public int sizeBytes() {
                                    return len;
                                }
                            };
                            break;
                        }
                    case "double":
                        if ( nfields==1 ) {
                            tt= new TransferType() {
                                @Override
                                public void write( HapiRecord record, int i, ByteBuffer buffer) {
                                    double d= record.getDouble(i);
                                    buffer.putDouble( d );
                                }
                                @Override
                                public int sizeBytes() {
                                    return 8;
                                }
                            };
                        } else {
                            tt= new TransferType() {
                                @Override
                                public void write( HapiRecord record, int i, ByteBuffer buffer) {
                                    double[] dd= record.getDoubleArray(i);
                                    for ( int j=0; j<nfields; j++ ) {
                                        double d= dd[j];
                                        buffer.putDouble( d );
                                    }
                                }
                                @Override
                                public int sizeBytes() {
                                    return 8 * nfields;
                                }
                            };                            
                        }
                        fl= getFill(parameter);
                        break;
                    case "integer":
                        if ( nfields==1 ) {
                            tt= new TransferType() {
                                @Override
                                public void write( HapiRecord record, int i, ByteBuffer buffer) {
                                    int integer= record.getInteger(i);
                                    buffer.putInt( integer );
                                }
                                @Override
                                public int sizeBytes() {
                                    return 4;
                                }
                            };
                        } else {
                            tt= new TransferType() {
                                @Override
                                public void write( HapiRecord record, int i, ByteBuffer buffer) {
                                    int[] dd= record.getIntegerArray(i);
                                    for ( int j=0; j<nfields; j++ ) {
                                        int d= dd[j];
                                        buffer.putInt( d );
                                    }
                                }
                                @Override
                                public int sizeBytes() {
                                    return 4 * nfields;
                                }
                            };                            
                        }
                        fl= getFill(parameter);
                        break;
                    default:
                    throw new IllegalArgumentException("server is misconfigured, using unsupported type: "+stype );
                }
                transferTypes[i]= tt;
                fill[i]= fl;

                totalFields+= nfields;
                bufferSize+= tt.sizeBytes();
            }

            b= ByteBuffer.allocate( bufferSize );
            b.order( ByteOrder.LITTLE_ENDIAN );
            
            sentRecordCount=0;
            
        } catch (JSONException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void sendRecord( OutputStream out, HapiRecord record ) throws IOException {

        for ( int i=0; i<record.length(); i++ ) {
            transferTypes[i].write( record, i, b );
        }
        byte[] bytes= b.array();
        
        if ( sentRecordCount==0 ) {
            if ( logger.isLoggable(Level.FINE)  ) {
                StringBuilder sbuf;
                sbuf = new StringBuilder();
                int nf= Math.min(80,bytes.length);
                for ( int i=0; i<nf; i++ ) {
                    sbuf.append( String.format( "%2d ", i ) );
                }
                logger.fine( sbuf.toString() );
                sbuf = new StringBuilder();
                for ( int i=0; i<nf; i++ ) {
                    sbuf.append( String.format( "%02x ", bytes[i] ) );
                }
                logger.fine( sbuf.toString() );
            }
        }
        out.write( bytes );
        
        b.flip();
        
        sentRecordCount++;
                
    }
    
    @Override
    public void finalize( OutputStream out ) {
        
    } 
    
}

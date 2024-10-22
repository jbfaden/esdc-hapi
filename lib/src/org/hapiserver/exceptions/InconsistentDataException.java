
package org.hapiserver.exceptions;

/**
 * The info response is not consistent with the data response, for
 * example when times are different lengths.
 * @author jbf
 */
public class InconsistentDataException extends RuntimeException {

    public InconsistentDataException( String msg ) {
        super(msg);
    }
    
}

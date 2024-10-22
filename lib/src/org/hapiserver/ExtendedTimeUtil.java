
package org.hapiserver;

import java.text.ParseException;

/**
 * Useful extentions to the TimeUtil class, like support for times like "now-P1D"
 * All of these could potentially go into a future version of the TimeUtil class.
 * @author jbf
 */
public class ExtendedTimeUtil {
    
    /**
     * year component position in seven element decomposed time array
     */
    public static final int YEAR = 0;
    
    /**
     * month component position in seven element decomposed time array
     */
    public static final int MONTH = 1;
    
    /**
     * day component position in seven element decomposed time array
     */
    public static final int DAY = 2;
    
    /**
     * hour component position in seven element decomposed time array
     */
    public static final int HOUR = 3;
    
    /**
     * minute component position in seven element decomposed time array
     */
    public static final int MINUTE = 4;
    
    /**
     * second component position in seven element decomposed time array
     */
    public static final int SECOND = 5;
    
    /**
     * nanosecond component position in seven element decomposed time array
     */
    public static final int NANOSECOND = 6;
    
    /**
     * parse the time which is known to the developer to be valid.  A runtime 
     * error is thrown if it it not valid.
     * @param time
     * @return the decomposed time.
     * @throws RuntimeException if the time is not valid.
     */
    public static int[] parseValidTime( String time ) {
        try {
            return parseTime(time);
        } catch ( ParseException ex ) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * parse the time, allowing times like "now" and "lastHour"
     * @param time
     * @return
     * @throws ParseException 
     */
    public static int[] parseTime( String time ) throws ParseException {
        if ( time.length()==0 ) {
            throw new IllegalArgumentException("empty time string");
        } else {
            if ( Character.isDigit(time.charAt(0) ) ) {
                //TODO: I didn't realize parseISO8601Time handles the now and last extensions.  This needs review.
                return TimeUtil.parseISO8601Time(time);
            } else {
                return labelledTime( time );
            }
        }
    }
    
    /**
     * allow one of the following:<ul>
     * <li>now
     * <li>lastHour
     * <li>lastDay
     * <li>lastMonth
     * <li>lastYear
     * <li>now-P1D
     * </ul>
     * @param label
     * @return 
     * @throws java.text.ParseException 
     */
    public static int[] labelledTime( String label ) throws ParseException {
        int[] now= TimeUtil.now();
        int[] delta= null;
        int i= label.indexOf("-");
        if ( i==-1 ) i= label.indexOf("+");
        if ( i>-1 ) {
            delta= TimeUtil.parseISO8601Duration(label.substring(i+1));
            if ( label.charAt(i)=='-') {
                for ( int j=0; j<TimeUtil.TIME_DIGITS; j++ ) {
                    delta[j]= -1 * delta[j];
                }
            }
            label= label.substring(0,i);
        }
        label= label.toLowerCase();
        if ( label.startsWith("last") ) {
            if ( label.endsWith("minute") ) {
                now[6]=0;
                now[5]=0;
            } else if ( label.endsWith("hour") ) {
                now[6]=0;
                now[5]=0;
                now[4]=0;
            } else if ( label.endsWith("day") ) {
                now[6]=0;
                now[5]=0;                
                now[4]=0;
                now[3]=0;
            } else if ( label.endsWith("month") ) {
                now[6]=0;
                now[5]=0;                
                now[4]=0;
                now[3]=0;
                now[2]=1;
            } else if ( label.endsWith("year") ) {
                now[6]=0;
                now[5]=0;                
                now[4]=0;
                now[3]=0;
                now[2]=1;
                now[1]=1;
            } else {
                throw new IllegalArgumentException("unsupported last component, must be one of minute, day, month, or year: "+label);
            }
        } else if ( label.equals("now") ) {
            //do nothing
        }
        if ( delta!=null ) {
            return TimeUtil.add( now, delta );
        }  else {
            return now;
        }
        
    }
    
}

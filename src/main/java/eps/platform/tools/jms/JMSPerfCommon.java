package eps.platform.tools.jms;

import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.naming.NamingException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;
@Slf4j
public class JMSPerfCommon
{
    private JMSPerfCommon() {}

    public static void onException(String msg, Exception ex)
    {
        log.error("Error:  Exception with sending message. {}", msg);
        log.error("\t" + ex.getMessage());
        
        String stacktrace = ExceptionUtils.getStackTrace(ex);
        log.error("\t" + stacktrace);
    }
    
    public static void onException(Message msg, Exception ex)
    {
        log.error("Error:  Exception with sending message. {}", msg.toString());
        log.error("\t" + ex.getMessage());
        
        String stacktrace = ExceptionUtils.getStackTrace(ex);
        log.error("\t" + stacktrace);
    }
    
    public static void createConnectionFactoryAndConnections(String factoryName, String serverUrl, String username, String password, int connections, List<Connection> connsVector) throws NamingException, JMSException
    {
    	if (connsVector == null) {
    		log.error("connsVector is null");
    		return;
    	}
    	
        // lookup the connection factory
        ConnectionFactory factory = null;
        if (factoryName != null)
        {
            JMSUtilities.initJNDI(serverUrl, username, password);
            factory = (ConnectionFactory) JMSUtilities.lookup(factoryName);
        }
        else 
        {
            factory = new com.tibco.tibjms.TibjmsConnectionFactory(serverUrl);
        }
        
        // create the connections
        connsVector.clear();
        for (int i=0;i<connections;i++)
        {
            Connection conn = factory.createConnection(username, password);
            conn.start();
            connsVector.add(conn);
        }
    }
        
    public static void createXAConnectionFactoryAndXAConnections(String factoryName, String serverUrl, String username, String password, int connections, List<Connection> connsVector) throws NamingException, JMSException
    {
    	if (connsVector == null) {
    		log.error("connsVector is null");
    		return;
    	}
    	
        // lookup the connection factory
        XAConnectionFactory factory = null;
        if (factoryName != null)
        {
            JMSUtilities.initJNDI(serverUrl, username, password);
            factory = (XAConnectionFactory) JMSUtilities.lookup(factoryName);
        }
        else 
        {
            factory = new com.tibco.tibjms.TibjmsXAConnectionFactory(serverUrl);
        }
        
        // create the connections
//        connsVector = new Vector<Connection>(connections);
        connsVector.clear();
        for (int i=0;i<connections;i++)
        {
            XAConnection conn = factory.createXAConnection(username,password);
            conn.start();
            connsVector.add(conn);
        }
    }

    public static void cleanup(List<Connection> connsVector) throws JMSException
    {
        // close the connections
    	for (Connection connection : connsVector) {
    		connection.close();
		}
//        for (int i=0;i<this.connections;i++) 
//        {
//            if (!xa)
//            {
//                Connection conn = connsVector.elementAt(i);
//                conn.close();
//            }
//            else
//            {
//                XAConnection conn = (XAConnection)connsVector.elementAt(i);
//                conn.close();
//            }
//        }
    }

    /**
     * Returns a connection, synchronized because of multiple prod/cons threads
     * @throws Exception 
     */
    public static synchronized Connection getConnection(List<Connection> connsVector, int connIter) throws Exception
    {
//      Connection connection = connsVector.elementAt(connIter++);
    	if (connsVector == null) {
    		throw new Exception("List null");
    	}
    	Connection connection = connsVector.get(connIter++);
        if (connIter == connsVector.size())
            connIter = 0;

        return connection;
    }

    /**
     * Returns a connection, synchronized because of multiple prod/cons threads
     */
//    public synchronized XAConnection getXAConnection()
//    {
//        XAConnection connection = (XAConnection)connsVector.elementAt(connIter++);
//        if (connIter == connections)
//            connIter = 0;
//
//        return connection;
//    }

    /**
     * Returns a destination, synchronized because of multiple prod/cons threads
     */
	public static synchronized Destination getDestination(Session s, String destName, boolean useTopic,
			boolean uniqueDests, int destIter) throws JMSException    {
        if (useTopic)
        {
            if (!uniqueDests)
                return s.createTopic(destName);
            else
                return s.createTopic(destName + "." + ++destIter);
        }
        else
        {
            if (!uniqueDests)
                return s.createQueue(destName);
            else
                return s.createQueue(destName + "." + ++destIter);
        }
    }

    /**
     * Returns a unique subscription name if durable
     * subscriptions are specified, synchronized because of multiple prod/cons threads
     */
    public static synchronized String getSubscriptionName(String durableName, int nameIter)
    {
        if (!StringUtils.isEmpty(durableName))
            return durableName + ++nameIter;
        else
            return null;
    }

    /**
     * Returns a txn helper object for beginning/commiting transaction
     * synchronized because of multiple prod/cons threads
     */
    public static synchronized  JMSTxnHelper getPerfTxnHelper(boolean xa)
    {
        return new JMSTxnHelper(xa);
    }

	public static int ackModeValue(String ackModeStr) {
		int ackMode = 0;
		if (ackModeStr.compareTo("DUPS_OK")==0)
            ackMode = javax.jms.Session.DUPS_OK_ACKNOWLEDGE;
        else if (ackModeStr.compareTo("AUTO")==0)
            ackMode = javax.jms.Session.AUTO_ACKNOWLEDGE;
        else if (ackModeStr.compareTo("CLIENT")==0)
            ackMode = javax.jms.Session.CLIENT_ACKNOWLEDGE;
        else if (ackModeStr.compareTo("EXPLICIT_CLIENT")==0)
            ackMode = com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_ACKNOWLEDGE;
        else if (ackModeStr.compareTo("EXPLICIT_CLIENT_DUPS_OK")==0)
            ackMode = com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE;
        else if (ackModeStr.compareTo("NO")==0)
            ackMode = com.tibco.tibjms.Tibjms.NO_ACKNOWLEDGE;
        else {
        	log.error("Error: invalid value of -ackMode parameter");
        	throw new IllegalArgumentException("Bad type.");
        }
		return ackMode;
	} 
    
    /**
     * Convert acknowledge mode to a string.
     */
    public static String ackModeName(int ackMode)
    {
        switch(ackMode)
        {
            case Session.DUPS_OK_ACKNOWLEDGE:      
                return "DUPS_OK_ACKNOWLEDGE";
            case Session.AUTO_ACKNOWLEDGE:         
                return "AUTO_ACKNOWLEDGE";
            case Session.CLIENT_ACKNOWLEDGE:       
                return "CLIENT_ACKNOWLEDGE";
            case com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_ACKNOWLEDGE:         
                return "EXPLICIT_CLIENT_ACKNOWLEDGE";
            case com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE:         
                return "EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE";
            case com.tibco.tibjms.Tibjms.NO_ACKNOWLEDGE:     
                return "NO_ACKNOWLEDGE";
            default:                                         
                return "(unknown)";
        }
    }
	
	public static int deliveryModeValue(String deliveryModeStr) {
		int deliveryMode = 0;
        if (deliveryModeStr.compareTo("PERSISTENT")==0)
            deliveryMode = javax.jms.DeliveryMode.PERSISTENT;
        else if (deliveryModeStr.compareTo("NON_PERSISTENT")==0)
            deliveryMode = javax.jms.DeliveryMode.NON_PERSISTENT;
        else if (deliveryModeStr.compareTo("RELIABLE")==0)
            deliveryMode = com.tibco.tibjms.Tibjms.RELIABLE_DELIVERY;
        else {
            log.error("Error: invalid value of -delivery parameter");
            throw new IllegalArgumentException("Bad type.");
        }
		return deliveryMode;
	} 
	
}


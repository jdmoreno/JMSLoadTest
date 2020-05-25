package eps.platform.tools.jms;

import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.naming.NamingException;
import javax.transaction.xa.XAResource;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JMSConsumer
    implements Runnable, com.tibco.tibjms.TibjmsMulticastExceptionListener
{
    // parameters
	private JMSConsumerParams params = null;
	
	private List<Connection> connsVector = new ArrayList<Connection>();
	private int connIter = 0;
	private int destIter = 0;
	private int nameIter = 0;
	
    // variables	
	@Getter private int recvCount;
	@Getter private long startTime;
	@Getter private long endTime;
	@Getter private long elapsed;
	@Getter private boolean stopNow;

    public JMSConsumer(JMSConsumerParams jmsConsumerParams, SSLParams ssl) {
    	this.params = jmsConsumerParams;
    	if (params.isInitialised()) {
	    	try {
				JMSUtilities.initSSLParams(params.getServerUrl(), ssl);
			} catch (JMSSecurityException ex) {
				JMSPerfCommon.onException("Exception", ex);
				params.setInitialised(false);
			}    	    	
	    	commonConstProcess();
    	}
    }

    /**
     * Constructor
     * 
     * @param args the command line arguments
     */
    public JMSConsumer(String[] args)
    {
    	this.params = new JMSConsumerParams(args);
    	if (params.isInitialised()) {
	    	try {
				JMSUtilities.initSSLParams(params.getServerUrl(), args);
			} catch (JMSSecurityException ex) {
				JMSPerfCommon.onException("Exception", ex);
				params.setInitialised(false);
			}
	    	commonConstProcess();
    	}
    }
    
    /**
     * Constructor
     * 
     * @param args the command line arguments
     */
    public void commonConstProcess()
    {
    	if (params == null || !params.isInitialised()) {
    		return;
    	}
    	
        try
        {
            printParameters();

			if (!params.isXa())
				JMSPerfCommon.createConnectionFactoryAndConnections(params.getFactoryName(),
						params.getServerUrl(), params.getUsername(),
						params.getPassword(), params.getConnections(), connsVector);
			else
				JMSPerfCommon.createXAConnectionFactoryAndXAConnections(params.getFactoryName(),
						params.getServerUrl(), params.getUsername(),
						params.getPassword(), params.getConnections(), connsVector);
            
			// create the consumer threads
            List<Thread> tv = new ArrayList<>(params.getThreads());
            for (int i=0;i<params.getThreads();i++)
            {
                Thread t = new Thread(this);
                tv.add(t);
                t.start();
            }

            // run for the specified amount of time
            if (params.getRunTime() > 0)
            {
                try 
                {
                    Thread.sleep(params.getRunTime() * 1000);
                } 
                catch (InterruptedException e) {}

                // ensure producer threads stop now
                stopNow = true;
                for (Thread thread : tv) {
                	thread.interrupt();
				}
            }

            // wait for the producer threads to exit
            for (Thread thread : tv) {
              try 
              {
            	  thread.join();
              } 
              catch (InterruptedException e) {}
			}

            // close connections
            JMSPerfCommon.cleanup(connsVector);

            // print performance
            printPerformance();
        }
        catch (NamingException | JMSException ex)
        {
        	JMSPerfCommon.onException("Exception", ex);
        }
    }

	private void printParameters() {
		log.info("------------------------------------------------------------------------");
		log.info("tibjmsMsgConsumerPerf SAMPLE");
		log.info("------------------------------------------------------------------------");
		log.info("Server....................... " + params.getServerUrl());
		log.info("User......................... " + params.getUsername());
		log.info("Destination.................. " + params.getDestName());
		if (params.getCount() > 0)
		    log.info("Count........................ " + params.getCount());
		if (params.getRunTime() > 0)
		    log.info("Duration..................... " + params.getRunTime());		
		log.info("Consumer Threads............. " + params.getThreads());
		log.info("Consumer Connections......... " + params.getConnections());
		log.info("Ack Mode..................... " + JMSPerfCommon.ackModeName(params.getAckMode()));
		log.info("Durable...................... " + (!StringUtils.isEmpty(params.getDurableName())));
		log.info("Selector..................... " + params.getSelector());
		log.info("XA........................... " + params.isXa());
		if (params.getTxnSize() > 0)
			log.info("Transaction Size............. " + params.getTxnSize());
		if (params.getFactoryName() != null)
			log.info("Connection Factory........... " + params.getFactoryName());
		log.info("------------------------------------------------------------------------");
	}

    /**
     * Update the total receive count.
     */
    private synchronized void countReceives(int count)
    {
        recvCount += count;
    }

    /**
     * The consumer thread's run method.
     */
    public void run()
    {
		Session session = null;
		MessageConsumer msgConsumer = null;
		String subscriptionName = JMSPerfCommon.getSubscriptionName(params.getDurableName(), nameIter);
		int msgCount = 0;
		Destination destination = null;
		XAResource xaResource = null;
		JMSTxnHelper txnHelper = JMSPerfCommon.getPerfTxnHelper(params.isXa());

        try 
        {
            Thread.sleep(250);
        }
        catch (InterruptedException e) {}

        try
        {
            if (!params.isXa())
            {
                // get the connection
                Connection connection = JMSPerfCommon.getConnection(connsVector, connIter);
                // create a session
                session = connection.createSession(params.getTxnSize() > 0, Session.AUTO_ACKNOWLEDGE);
            }
            else
            {
                // get the connection
                XAConnection connection = (XAConnection) JMSPerfCommon.getConnection(connsVector, connIter);            
                // create a session
                session = connection.createXASession();
            }

            if (params.isXa())
                /* get the XAResource for the XASession */
                xaResource = ((javax.jms.XASession)session).getXAResource();
        	
            // get the destination
            destination = JMSPerfCommon.getDestination(session, params.getDestName(), params.isUseTopic(), params.isUniqueDests(), destIter);
            
            // create the consumer
            if (subscriptionName == null)
                msgConsumer = session.createConsumer(destination, params.getSelector());
            else
                msgConsumer = session.createDurableSubscriber((Topic)destination,
                                                              subscriptionName,
                                                              params.getSelector(),
                                                              false);

            // register multicast exception listener for multicast consumers
            if (com.tibco.tibjms.Tibjms.isConsumerMulticast(msgConsumer))
                com.tibco.tibjms.Tibjms.setMulticastExceptionListener(this);

            // receive messages
            while ((params.getCount() == 0 || msgCount < (params.getCount()/params.getThreads())) && !stopNow)
            {
                // a no-op for local txns
                txnHelper.beginTx(xaResource);

                Message msg = msgConsumer.receive();
                if (msg == null)
                    break;

                if (msgCount == 0)
                    startTiming();

                msgCount++;
                
                // acknowledge the message if necessary
                if (params.getAckMode() == Session.CLIENT_ACKNOWLEDGE ||
                	params.getAckMode() == com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_ACKNOWLEDGE ||
    				params.getAckMode() == com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE)
                {
                    msg.acknowledge();
                }

                // commit the transaction if necessary
                if (params.getTxnSize() > 0 && msgCount % params.getTxnSize() == 0)
                    txnHelper.commitTx(xaResource, session);
                
                
                // force the uncompression of compressed messages
                if (msg.getBooleanProperty("JMS_TIBCO_COMPRESS"))
                    ((BytesMessage) msg).getBodyLength();
            }
        }
        catch (JMSException ex)
        {
            if (!stopNow)
            {
            	JMSPerfCommon.onException("Exception", ex);

                Exception le = ex.getLinkedException();
                if (le != null)
                {
                	JMSPerfCommon.onException("Linked Exception", ex);
                }
            }
        } catch (Exception ex) {
        	JMSPerfCommon.onException("Exception", ex);
		}

        // commit any remaining messages
        if (params.getTxnSize() > 0) {
            try 
            {
                txnHelper.commitTx(xaResource, session);
            }
            catch (JMSException ex) 
            {
                if (!stopNow)
                	JMSPerfCommon.onException("Exception", ex);
            }
        }
        
        stopTiming();

        countReceives(msgCount);

		try {
			if (msgConsumer != null)
				msgConsumer.close();

			if (session != null) {
				if (subscriptionName != null) {
					session.unsubscribe(subscriptionName);
				}
				session.close();
			}
		} catch (JMSException ex) {
			JMSPerfCommon.onException("Exception clossing ", ex);
		}
    }

    private synchronized void startTiming()
    {
        if (startTime == 0)
            startTime = System.currentTimeMillis();
    }
    
    private synchronized void stopTiming()
    {
        endTime = System.currentTimeMillis();
    }
    
    /**
     * Get the performance results.
     */
    private void printPerformance() 
    {
        if (endTime > startTime)
        {
            elapsed = endTime - startTime;
            double seconds = elapsed/1000.0;
            int perf = (int)((recvCount * 1000.0)/elapsed);
            log.info (recvCount + " times took " + seconds + " seconds, performance is " + perf + " messages/second");
        }
        else
        {
        	log.info("interval too short to calculate a message rate");
        }
    }

    /**
     * Multicast exception listener
     */
    public void onMulticastException(Connection connection, Session session,
                                     MessageConsumer consumer, JMSException exception)
    {
        JMSPerfCommon.onException("onMulticastException", exception);
        try
        {
            session.close();
        }
        catch (JMSException closeEx)
        {
            // ignore
        }
    }

    /**
     * main
     */
    public static void main(String[] args)
    {
    	JMSConsumer t = new JMSConsumer(args);
    }
}

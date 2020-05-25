package eps.platform.tools.jms;

/* 
 * Copyright (c) 2002-2016 TIBCO Software Inc. 
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 * 
 * $Id: tibjmsMsgProducerPerf.java 90180 2016-12-13 23:00:37Z $
 * 
 */

/*
 * This is a sample message producer class used to measure performance.
 *
 * For the the specified number of threads this sample creates a 
 * session and a message producer for the specified destination.
 * Once the specified number of messages are produced the performance
 * results are printed and the program exits.
 *
 * Usage:  java tibjmsMsgProducerPerf  [options]
 *
 *  where options are:
 *
 *   -server       <url>         EMS server URL. Default is
 *                               "tcp://localhost:7222".
 *   -user         <username>    User name. Default is null.
 *   -password     <password>    User password. Default is null.
 *   -topic        <topic-name>  Topic name. Default is "topic.sample".
 *   -queue        <queue-name>  Queue name. No default.
 *   -size         <num bytes>   Message payload size in bytes. Default is 100.
 *   -count        <num msgs>    Number of messages to send. Default is 10k.
 *   -time         <seconds>     Number of seconds to run. Default is 0 (forever).
 *   -delivery     <mode>        Delivery mode. Default is NON_PERSISTENT.
 *                               Other values: PERSISTENT and RELIABLE.
 *   -threads      <num threads> Number of message producer threads. Default is 1.
 *   -connections  <num conns>   Number of message producer connections. Default is 1.
 *   -txnsize      <num msgs>    Number of nessages per producer transaction. Default is 0.
 *   -rate         <msg/sec>     Message rate for producer threads.
 *   -payload      <file name>   File containing message payload.
 *   -factory      <lookup name> Lookup name for connection factory.
 *   -uniquedests                Each producer thread uses a unique destination.
 *   -compression                Enable message compression.
 *   -xa                         Use XA transactions.
 */
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.naming.NamingException;
import javax.transaction.xa.XAResource;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JMSProducer
    implements Runnable 
{
		
    // parameters
	private JMSProducerParams params = null;
	
	private List<Connection> connsVector = new ArrayList<>();
	private int connIter = 0;
	private int destIter = 0;

    // variables
    @Getter private int sentCount;
    @Getter private long startTime;
    @Getter private long endTime;
    @Getter private long completionTime;
    @Getter private long elapsed;
    @Getter private boolean stopNow;
    
    class TibjmsCompletionListener implements CompletionListener
    {
        private int     completionCount = 0;
        private boolean finished        = false;
        private Object  finishedLock    = new Object();
        private int     finishCount     = 0;
        
        void waitUntilFinished()
        {
            synchronized (finishedLock)
            {
                while (!finished)
                {
                    try
                    {
                        finishedLock.wait();
                    }
                    catch (InterruptedException e)
                    {
                        finished = true;
                    }
                }
            }
        }
        
        void setFinishCount(int count)
        {
            synchronized (finishedLock)
            {
                finishCount = count;
                if (completionCount >= finishCount)
                    finished = true;
            }
        }
        
        public void onCompletion(Message msg)
        {
            completionCount++;
            
            synchronized (finishedLock)
            {
                if (finished)
                    return;
                
                if (finishCount > 0 && completionCount >= finishCount)
                {
                    finished = true;
                    completionTime = System.currentTimeMillis();
                    finishedLock.notify();
                }
            }
        }


        @Override
		public void onException(Message message, Exception exception) {
        	JMSPerfCommon.onException(message, exception);
    	}
    }
 
    public JMSProducer(JMSProducerParams jmsProducerParams, SSLParams ssl) {
    	this.params = jmsProducerParams;
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
    public JMSProducer(String[] args) {
    	this.params = new JMSProducerParams(args);
    	
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

    private void commonConstProcess() {
    	if (params == null || !params.isInitialised()) {
    		return;
    	}

		try
        {
            // print parameters
            printParameters();

			if (!params.isXa())
				JMSPerfCommon.createConnectionFactoryAndConnections(params.getFactoryName(),
						params.getServerUrl(), params.getUsername(),
						params.getPassword(), params.getConnections(), connsVector);
			else
				JMSPerfCommon.createXAConnectionFactoryAndXAConnections(params.getFactoryName(),
						params.getServerUrl(), params.getUsername(),
						params.getPassword(), params.getConnections(), connsVector);

            // create the producer threads
            List<Thread> tv = new ArrayList<Thread>(params.getThreads());
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
		log.info("tibjmsMsgProducerPerf SAMPLE");
		log.info("------------------------------------------------------------------------");
		log.info("Server....................... " + params.getServerUrl());
		log.info("User......................... " + params.getUsername());
		log.info("Destination.................. " + params.getDestName());
		if (params.getCount() > 0)
		    log.info("Count........................ " + params.getCount());
		if (params.getRunTime() > 0)
		    log.info("Duration..................... " + params.getRunTime());
		log.info("Production Threads........... " + params.getThreads());
		log.info("Production Connections....... " + params.getConnections());
		log.info("Message Size................. " + (params.getPayloadFile() != null ? params.getPayloadFile() : String.valueOf(params.getMsgSize())));				
		log.info("DeliveryMode................. " + deliveryModeName(params.getDeliveryMode()));
		log.info("Compression.................. " + params.isCompression());
		log.info("XA........................... " + params.isXa());
		log.info("Asynchronous Sending......... " + params.isAsync());
		if (params.getMsgRate() > 0)
		    log.info("Message Rate................. " + params.getMsgRate());
		if (params.getTxnSize() > 0)
		    log.info("Transaction Size............. " + params.getTxnSize());
		if (params.getFactoryName() != null)
		    log.info("Connection Factory........... " + params.getFactoryName());
		log.info("------------------------------------------------------------------------");
	}

    /**
     * Update the total sent count.
     */
    private synchronized void countSends(int count)
    {
        sentCount += count;
    }

    /** 
     * The producer thread's run method.
     */
    public void run()
    {
        int msgCount = 0;
        MsgRateChecker msgRateChecker = null;
        TibjmsCompletionListener cl   = null;
        
        try
        {
            Thread.sleep(500);
        }
        catch (InterruptedException e) {}

        MessageProducer msgProducer = null;
        Session session = null;        
        try
        {
            Destination destination = null;
            XAResource xaResource = null;
            JMSTxnHelper txnHelper = JMSPerfCommon.getPerfTxnHelper(params.isXa());
            
            if (params.isAsync())
                cl = new TibjmsCompletionListener();

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
            
            // create the producer
            msgProducer = session.createProducer(destination);

            // set the delivery mode
            msgProducer.setDeliveryMode(params.getDeliveryMode());
            
            // performance settings
            msgProducer.setDisableMessageID(true);
            msgProducer.setDisableMessageTimestamp(true);

            // create the message
            Message msg = createMessage(session);

            // enable compression if necessary
            if (params.isCompression())
                msg.setBooleanProperty("JMS_TIBCO_COMPRESS", true); 

            // initialize message rate checking
            if (params.getMsgRate() > 0)
                msgRateChecker = new MsgRateChecker(params.getMsgRate());
            
            startTiming();
            
            // publish messages
            while ((params.getCount() == 0 || msgCount < (params.getCount()/params.getThreads())) && !stopNow)
            {
                // a no-op for local txns
                txnHelper.beginTx(xaResource);

                if (params.isAsync()) {
                    msgProducer.send(msg, cl);
                } else {
                    msgProducer.send(msg);
                }

                msgCount++;

                // commit the transaction if necessary
                if (params.getTxnSize() > 0 && msgCount % params.getTxnSize() == 0)
                    txnHelper.commitTx(xaResource, session);
                
                // check the message rate
                if (params.getMsgRate() > 0)
                    msgRateChecker.checkMsgRate(msgCount);
            }
            
            // commit any remaining messages
            if (params.getTxnSize() > 0)
                txnHelper.commitTx(xaResource, session);
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
        finally {
        	if (msgProducer!= null) {
				try {
					msgProducer.close();
				} catch (JMSException e) {}
        	}
        	
        	if (session!= null) {
				try {
					session.close();
				} catch (JMSException e) {}
        	}
        	
        }

        // let the completion listener know when to finish.
        if (params.isAsync())
            cl.setFinishCount(msgCount);
        
        stopTiming(cl);
        countSends(msgCount);
    }

    /**
     * Create the message.
     */
    private Message createMessage(Session session) throws JMSException
    {
        String payload = null;
        
        // create the message
        BytesMessage msg = session.createBytesMessage();

        // add the payload
        if (params.getPayloadFile() != null)
        {
            try (InputStream instream = new BufferedInputStream(new FileInputStream(params.getPayloadFile())))  {
                int size = instream.available();
                byte[] bytesRead = new byte[size];
                instream.read(bytesRead);
                payload = new String(bytesRead);                
            }
            catch (IOException ex)
            {
            	JMSPerfCommon.onException("Error: unable to load payload file", ex);

            }
        }
        else if (params.getMsgSize() > 0)
        {
        	payload = StringUtils.repeat('a', params.getMsgSize());
        }
        
        if (payload != null)
        {
            // add the payload to the message
            msg.writeBytes(payload.getBytes());
        }
        
        return msg;
    }

    private synchronized void startTiming()
    {
        if (startTime == 0)
            startTime = System.currentTimeMillis();
    }
    
    private synchronized void stopTiming(TibjmsCompletionListener cl)
    {
        endTime = System.currentTimeMillis();
        if (cl != null)
        {
            cl.waitUntilFinished();
            completionTime = System.currentTimeMillis();
        }
    }

    /**
     * Convert delivery mode to a string.
     */
    private static String deliveryModeName(int mode)
    {
        switch(mode)
        {
            case javax.jms.DeliveryMode.PERSISTENT:         
                return "PERSISTENT";
            case javax.jms.DeliveryMode.NON_PERSISTENT:     
                return "NON_PERSISTENT";
            case com.tibco.tibjms.Tibjms.RELIABLE_DELIVERY: 
                return "RELIABLE";
            default:                                        
                return "(unknown)";
        }
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
            int perf = (int)((sentCount * 1000.0)/elapsed);
            log.info(sentCount + " times took " + seconds + " seconds, performance is " + perf + " messages/second");
        }
        else
        {
        	log.info("interval too short to calculate a message rate");
        }
        
        if (params.isAsync())
        {
            long completionElapsed;
            
            if (completionTime > startTime)
            {
                completionElapsed = completionTime - startTime;
                double seconds = completionElapsed/1000.0;
                int perf = (int)((sentCount * 1000.0)/completionElapsed);
                log.info(sentCount +
                    " completion listener calls took " + seconds +
                    " seconds, performance is " + perf + " messages/second");
            }
            else
            {
                log.info("interval too short to calculate a completion listener rate");
            }
        }
    }
    
    /**
     * main
     */
    public static void main(String[] args)
    {
        JMSProducer t = new JMSProducer(args);
    }
}

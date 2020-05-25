package eps.platform.tools.jms;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Helper class for beginning/commiting transactions, maintains
 * any requried state. Each prod/cons thread needs to get an instance of 
 * this by calling getPerfTxnHelper().
 */
public class JMSTxnHelper
{
    public boolean startNewXATxn = true;
    public Xid     xid = null;
    public boolean xa = false;

    public JMSTxnHelper(boolean xa)
    { 
        this.xa = xa;
    }

    public void beginTx(XAResource xaResource) throws JMSException
    {
        if (xa && startNewXATxn)
        {
            /* create a transaction id */
            java.rmi.server.UID uid = new java.rmi.server.UID();
            this.xid = new com.tibco.tibjms.TibjmsXid(0, uid.toString(), "branch");
            
            /* start a transaction */
            try
            {
                xaResource.start(xid, XAResource.TMNOFLAGS);
            }
            catch (XAException e)
            {
                System.err.println("XAException: " + " errorCode=" + e.errorCode);
                e.printStackTrace();
                System.exit(0);
            }
            startNewXATxn = false;
        }
    }
    
    public void commitTx(XAResource xaResource, Session session) throws JMSException
    {
        if (xa)
        {
            if (xaResource != null && xid != null)
            {
                /* end and prepare the transaction */
                try
                {
                    xaResource.end(xid, XAResource.TMSUCCESS);
                    xaResource.prepare(xid);
                }
                catch (XAException e) 
                {
                    System.err.println("XAException: " + " errorCode=" + e.errorCode);
                    e.printStackTrace();
                    
                    Throwable cause = e.getCause();
                    if (cause != null)
                    {
                        System.err.println("cause: ");
                        cause.printStackTrace();
                    }
                    
                    try
                    { 
                        xaResource.rollback(xid); 
                    }
                    catch (XAException re) {}
                    
                    System.exit(0);
                }
                
                /* commit the transaction */
                try
                {
                    xaResource.commit(xid, false);
                } 
                catch (XAException e) 
                {
                    System.err.println("XAException: " + " errorCode=" + e.errorCode);
                    e.printStackTrace();
                    
                    Throwable cause = e.getCause();
                    if (cause != null)
                    {
                        System.err.println("cause: ");
                        cause.printStackTrace();
                    }
                    
                    System.exit(0);
                }
                startNewXATxn = true;
                xid = null;
            }
        }
        else
        {
            session.commit();
        }
    }
}
package eps.platform.tools.jms;

import java.util.Hashtable;

/* 
 * Copyright (c) 2001-2019 TIBCO Software Inc. 
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 * 
 * $Id: tibjmsUtilities.java 109379 2019-04-12 17:08:12Z $
 * 
 */

/*
 * This sample uses JNDI to retrieve administered objects.
 *
 * Optionally all parameters hardcoded in this sample can be
 * read from the jndi.properties file.
 *
 * This file also contains an SSL parameter helper class to enable
 * an SSL connection to a TIBCO Enterprise Message Service server.
 *
 */
import javax.jms.JMSSecurityException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JMSUtilities
{
    static Context jndiContext = null;

    static final String  providerContextFactory =
                            "com.tibco.tibjms.naming.TibjmsInitialContextFactory";

    static final String  defaultProtocol = "tibjmsnaming";

    static final String  defaultProviderURL =
                            defaultProtocol + "://localhost:7222";

    private JMSUtilities() {    	
    }
    
    public static void initJNDI(String providerURL) throws NamingException
    {
        initJNDI(providerURL,null,null);
    }

    public static void initJNDI(String providerURL, String userName,
        String password) throws NamingException
    {
        if (jndiContext != null)
            return;

        if (providerURL == null || (providerURL.length() == 0))
            providerURL = defaultProviderURL;

        try
        {
        	Hashtable<String,String> env = new Hashtable<>();

            env.put(Context.INITIAL_CONTEXT_FACTORY,providerContextFactory);
            env.put(Context.PROVIDER_URL,providerURL);

            if (userName != null)
            {
                env.put(Context.SECURITY_PRINCIPAL,userName);

                if (password != null)
                    env.put(Context.SECURITY_CREDENTIALS,password);
            }

            jndiContext = new InitialContext(env);
        }
        catch (NamingException e)
        {
            log.error("Failed to create JNDI InitialContext with provider URL set to " + providerURL+", error = "+e.toString());
            throw e;
        }
    }

    public static Object lookup(String objectName) throws NamingException
    {
        if (objectName == null)
            throw new IllegalArgumentException("null object name not legal");

        if (objectName.length() == 0)
            throw new IllegalArgumentException("empty object name not legal");

        /*
         * check if not initialized, then initialize
         * with default parameters
         */
        initJNDI(null);

        /*
         * do the lookup
         */
        return jndiContext.lookup(objectName);
    }


    public static SSLParams initSSLParams(String serverUrl,String[] args)
        throws JMSSecurityException
    {
    	SSLParams ssl = null;
        if (serverUrl != null && serverUrl.indexOf("ssl://") >= 0)
        {
           ssl = new SSLParams(args);
			if (ssl.isInitialised()) {
			 	   ssl.init();
			}           
        }
        return ssl;
    }

	public static SSLParams initSSLParams(String serverUrl, SSLParams ssl) throws JMSSecurityException {
		if (serverUrl != null && serverUrl.indexOf("ssl://") >= 0 && ssl != null && ssl.isInitialised()) {
			ssl.init();
		}
		return ssl;
	}
}




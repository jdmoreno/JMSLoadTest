package eps.platform.tools.jms;

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSSecurityException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SSLParams 
    implements com.tibco.tibjms.TibjmsSSLHostNameVerifier
{
	@Getter private boolean initialised = false;
	
    @Getter String          ssl_vendor                  = null;
    @Getter boolean         ssl_trace                   = false;
    @Getter boolean         ssl_debug_trace             = false;
    @Getter boolean         ssl_verify_hostname         = false;
    @Getter String          ssl_expected_hostname       = null;
    @Getter List<String>    ssl_trusted                 = null;
    @Getter List<String>    ssl_issuers                 = null;
    @Getter String          ssl_identity                = null;
    @Getter String          ssl_private_key             = null;
    @Getter String          ssl_password                = null;
    @Getter boolean         ssl_custom                  = false;
    @Getter String          ssl_ciphers                 = null;

    /**
     * Constructor
     *
     * @param args the command line arguments
     */
    public SSLParams(
        String          ssl_vendor,
        boolean         ssl_trace,
        boolean         ssl_debug_trace,
        boolean         ssl_verify_hostname,
        String          ssl_expected_hostname,
        List<String>    ssl_trusted,
        List<String>    ssl_issuers,
        String          ssl_identity,
        String          ssl_private_key,
        String          ssl_password,
        boolean         ssl_custom,
        String          ssl_ciphers)
    {
    	this.ssl_vendor = ssl_vendor;
    	this.ssl_trace = ssl_trace;
    	this.ssl_debug_trace = ssl_debug_trace;
    	this.ssl_verify_hostname = ssl_verify_hostname;
    	this.ssl_expected_hostname = ssl_expected_hostname;
    	this.ssl_trusted = ssl_trusted;
    	this.ssl_issuers = ssl_issuers;
    	this.ssl_identity = ssl_identity;
    	this.ssl_private_key = ssl_private_key;
    	this.ssl_password = ssl_password;
    	this.ssl_custom = ssl_custom;
    	this.ssl_ciphers = ssl_ciphers;
    	
    	initialised = true;
    }
    
    public SSLParams(String[] args) {
    	initialised = parseArgs(args);
    	if (!initialised) {
    		usage();
    	}    	
    }
    
    public boolean parseArgs(String[] args)
    {
        int     trusted_pi      = 0;
        String  trusted_suffix  = "";
        int     issuer_pi      = 0;
        String  issuer_suffix  = "";

        int i=0;
        while (i < args.length)
        {
            if (args[i].compareTo("-ssl_vendor")==0)
            {
                if ((i+1) >= args.length) usage();
                ssl_vendor = args[i+1];
                i += 2;
            }
            else
            if (args[i].compareTo("-ssl_trace")==0)
            {
                ssl_trace = true;
                i += 1;
            }
            else
            if (args[i].compareTo("-ssl_debug_trace")==0)
            {
                ssl_debug_trace = true;
                i += 1;
            }
            else
            if (args[i].compareTo("-ssl_expected_hostname")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                ssl_expected_hostname = args[i+1];
                i += 2;
            }
            else
            if (args[i].compareTo("-ssl_verify_hostname")==0)
            {
                ssl_verify_hostname = true;
                i += 1;
            }
            else
            if (args[i].compareTo("-ssl_custom")==0)
            {
                ssl_custom = true;
                i += 1;
            }
            else
            if (args[i].compareTo("-ssl_ciphers")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                ssl_ciphers = args[i+1];
                i += 2;
            }
            else
            if (args[i].compareTo("-ssl_identity")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                ssl_identity = args[i+1];
                i += 2;
            }
            else
            if (args[i].compareTo("-ssl_private_key")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                ssl_private_key = args[i+1];
                i += 2;
            }
            else
            if (args[i].compareTo("-ssl_password")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                ssl_password = args[i+1];
                i += 2;
            }
            else
            if (args[i].compareTo("-ssl_trusted"+trusted_suffix)==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                String cert = args[i+1];
                if (cert == null) continue;
                if (ssl_trusted == null)
                	ssl_trusted = new ArrayList<String>();
//                    ssl_trusted = new Vector<String>();
                ssl_trusted.add(cert);
                trusted_pi++;
                trusted_suffix = String.valueOf(trusted_pi);
                i += 2;
            }
            else
            if (args[i].compareTo("-ssl_issuer"+issuer_suffix)==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                String cert = args[i+1];
                if (cert == null) continue;
                if (ssl_issuers == null)
                	ssl_issuers = new ArrayList<String>();
//                    ssl_issuers = new Vector<String>();
                ssl_issuers.add(cert);
                issuer_pi++;
                issuer_suffix = String.valueOf(issuer_pi);
                i += 2;
            }
            else
            {
                i++;
            }
        }
        return true;
    }

    public void init() throws JMSSecurityException
    {
        if (ssl_trace)
            com.tibco.tibjms.TibjmsSSL.setClientTracer(System.err);

        if (ssl_debug_trace)
            com.tibco.tibjms.TibjmsSSL.setDebugTraceEnabled(true);

        if (ssl_vendor != null)
            com.tibco.tibjms.TibjmsSSL.setVendor(ssl_vendor);

        if (ssl_expected_hostname != null)
            com.tibco.tibjms.TibjmsSSL.setExpectedHostName(ssl_expected_hostname);

        if (ssl_custom)
            com.tibco.tibjms.TibjmsSSL.setHostNameVerifier(this);

        if (!ssl_verify_hostname)
            com.tibco.tibjms.TibjmsSSL.setVerifyHostName(false);

        if (ssl_trusted != null)
        {
        	for (String certfile : ssl_trusted) {
        		com.tibco.tibjms.TibjmsSSL.addTrustedCerts(certfile);
			}
//            for (int i=0; i<ssl_trusted.size(); i++)
//            {
//                String certfile = ssl_trusted.elementAt(i);
//                com.tibco.tibjms.TibjmsSSL.addTrustedCerts(certfile);
//            }
        }
        else
        {
            com.tibco.tibjms.TibjmsSSL.setVerifyHost(false);
        }

        if (ssl_issuers != null)
        {
        	for (String certfile : ssl_issuers) {
        		com.tibco.tibjms.TibjmsSSL.addIssuerCerts(certfile);
			}
        	
//            for (int i=0; i<ssl_issuers.size(); i++)
//            {
//                String certfile = ssl_issuers.elementAt(i);
//                com.tibco.tibjms.TibjmsSSL.addIssuerCerts(certfile);
//            }
        }

        if (ssl_identity != null)
        {
            com.tibco.tibjms.TibjmsSSL.setIdentity(
                ssl_identity,ssl_private_key,
                ssl_password.toCharArray());
        }
        else if (ssl_password != null)
        {
            com.tibco.tibjms.TibjmsSSL.setIdentity(
                null,null,ssl_password.toCharArray());
        }

        if (ssl_ciphers != null)
            com.tibco.tibjms.TibjmsSSL.setCipherSuites(ssl_ciphers);
    }

    public void verifyHostName(String connectedHostName,
                               String expectedHostName,
                               String certificateCN,
                               java.security.cert.X509Certificate server_certificate)
                throws JMSSecurityException
    {
        log.info("HostNameVerifier: "+
                "    connected = ["+connectedHostName+"]\n"+
                "    expected  = ["+expectedHostName+"]\n"+
                "    certCN    = ["+certificateCN+"]");

        return;
    }
    
    public static void usage()
    {
        log.info("\nSSL options:");
        log.info("");
        log.info(" -ssl_vendor               <name>      - SSL vendor: 'j2se' (the default)");
        log.info(" -ssl_trace                            - trace SSL initialization");
        log.info(" -ssl_vendor_trace                     - trace SSL handshake and related");
        log.info(" -ssl_trusted[n]           <file-name> - file with trusted certificates,");
        log.info("                                         this parameter may repeat if more");
        log.info("                                         than one file required");
        log.info(" -ssl_verify_hostname                  - do not verify certificate name.");
        log.info("                                         (this disabled by default)");
        log.info(" -ssl_expected_hostname    <string>    - expected name in the certificate");
        log.info(" -ssl_custom                           - use custom verifier (it shows names");
        log.info("                                         always approves them).");
        log.info(" -ssl_identity             <file-name> - client identity file");
        log.info(" -ssl_issuer[n]            <file-name> - client issuer file");
        log.info(" -ssl_private_key          <file-name> - client key file (optional)");
        log.info(" -ssl_password             <string>    - password to decrypt client identity");
        log.info("                                         or key file");
        log.info(" -ssl_ciphers              <suite-name(s)> - cipher suite names, colon separated");
//        System.exit(0);
    }
}

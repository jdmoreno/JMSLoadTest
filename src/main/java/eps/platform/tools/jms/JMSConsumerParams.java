package eps.platform.tools.jms;

import javax.jms.Session;

import org.apache.commons.lang3.StringUtils;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder(buildMethodName = "build", toBuilder = true, access = AccessLevel.PUBLIC, setterPrefix = "set")
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class JMSConsumerParams {

	@Setter @Getter @Builder.Default private boolean initialised = false;
	
	// parameters
	@Getter @Builder.Default private String serverUrl = "tcp://localhost:7222";
	@Getter @Builder.Default private String username = null;
	@Getter @Builder.Default private String password = null;
	@Getter @Builder.Default private int connections = 1;	
	@Getter @Builder.Default private String destName = "topic.sample";
	@Getter @Builder.Default private String factoryName = null;	
	
	@Getter @Builder.Default private boolean useTopic = true;	
	@Getter @Builder.Default private boolean uniqueDests = false;
	@Getter @Builder.Default private boolean xa = false;
	
	@Getter @Builder.Default private String selector = null;
	@Getter @Builder.Default private int txnSize = 0;
	@Getter @Builder.Default private int count = 10000;
	@Getter @Builder.Default private int runTime = 0;
	@Getter @Builder.Default private int threads = 1;
	@Getter @Builder.Default private int ackMode = Session.AUTO_ACKNOWLEDGE;
	
	@Getter @Builder.Default private String durableName = "";
	
    public JMSConsumerParams(String[] args)
    {
    	initialised = parseArgs(args);
    	if (!initialised) {
    		usage();
    	}
    }
	
    // More initialisation stuff later
    public void initialise() {
    	initialised = true;
    }
    
    /**
     * Parse the command line arguments.
     */
    private boolean parseArgs(String[] args)
    {
        int i=0;

        while (i < args.length)
        {
            if (args[i].compareTo("-server")==0)
            {
                if ((i+1) >= args.length) { 
                	return false;
                }
                serverUrl = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-topic")==0)
            {
                if ((i+1) >= args.length) { 
                	return false;
                }
                destName = args[i+1];
                useTopic = true;
                i += 2;
            }
            else if (args[i].compareTo("-queue")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                destName = args[i+1];
                useTopic = false;
                i += 2;
            }
            else if (args[i].compareTo("-durable")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                durableName = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-user")==0)
            {
                if ((i+1) >= args.length) {
	             	return false;
                }
                username = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-help-ssl")==0)
            {
            	SSLParams.usage();
            }
            else if (args[i].compareTo("-password")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                password = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-uniquedests")==0)
            {
                uniqueDests = true;
                i += 1;
            }
            else if (args[i].compareTo("-xa")==0)
            {
                xa = true;
                i += 1;
            }
            else if (args[i].compareTo("-threads")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                try 
                {
                    threads = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e) 
                {
                    log.error("Error: invalid value of -threads parameter");
                    return false;
                }
                if (threads < 1)
                {
                	log.error("Error: invalid value of -threads parameter, must be >= 1");
                    return false;
                }
                i += 2;
            }
            else if (args[i].compareTo("-connections")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                try 
                {
                    connections = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e) 
                {
                	log.error("Error: invalid value of -connections parameter");
                    return false;
                }
                if (connections < 1) 
                {
                	log.error("Error: invalid value of -connections parameter, must be >= 1");
                    return false;
                }
                i += 2;
            }
            else if (args[i].compareTo("-count")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                try 
                {
                    count = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e) 
                {
                	log.error("Error: invalid value of -count parameter");
                    return false;
                }
                i += 2;
            }
            else if (args[i].compareTo("-time")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                try 
                {
                    runTime = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e) 
                {
                	log.error("Error: invalid value of -time parameter");
                    return false;
                }
                i += 2;
            }
            else if (args[i].compareTo("-ackmode")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                String dm = args[i+1];
                i += 2;
                try {
                	ackMode = JMSPerfCommon.ackModeValue(dm);
                } catch (IllegalArgumentException e) {
                	return false;
	    		}
            }
            else if (args[i].compareTo("-txnsize")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                try 
                {
                    txnSize = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e) 
                {
                	log.error("Error: invalid value of -txnsize parameter");
                    return false;
                }
                if (txnSize < 1) 
                {
                	log.error("Error: invalid value of -txnsize parameter");
                    return false;
                }
                i += 2;
            }
            else if (args[i].compareTo("-selector")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                selector = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-factory")==0)
            {
                if ((i+1) >= args.length) {
                	return false;
                }
                factoryName = args[i+1];
                i += 2;
            }
            else if (args[i].startsWith("-ssl"))
            {
                i += 2;
            }
            else 
            {
            	log.error("Error: invalid option: " + args[i]);
                return false;
            }
        }

        if (!StringUtils.isEmpty(durableName) && !useTopic)
        {
        	log.error("Error: -durable cannot be used with -queue");
            return false;
        }
        return true;
    }

    private void usage()
    {
        log.info("Usage: java tibjmsMsgConsumerPerf [options] [ssl options]");
        log.info("  where options are:");
        log.info("    -server       <url>         - Server URL. Default is \"tcp://localhost:7222\".");
        log.info("    -user         <username>    - User name. Default is null.");
        log.info("    -password     <password>    - User password. Default is null.");
        log.info("    -topic        <topic-name>  - Topic name. Default is \"topic.sample\".");
        log.info("    -queue        <queue-name>  - Queue name. No default.");
        log.info("    -count        <num msgs>    - Number of messages to consume. Default is 10k.");
        log.info("    -time         <seconds>     - Number of seconds to run. Default is 0.");
        log.info("    -threads      <num threads> - Number of consumer threads. Default is 1.");
        log.info("    -connections  <num conns>   - Number of consumer connections. Default is 1.");
        log.info("    -txnsize      <num msgs>    - Number of messages per consumer transaction.");
        log.info("    -durable      <name>        - Durable subscription name. No default.");
        log.info("    -selector     <selector>    - Message selector for consumers. No default.");
        log.info("    -ackmode      <mode>        - Message acknowledge mode. Default is AUTO.");
        log.info("                                  Other values: DUPS_OK, CLIENT EXPLICIT_CLIENT,");
        log.info("                                  EXPLICIT_CLIENT_DUPS_OK and NO.");
        log.info("    -factory      <lookup name> - Lookup name for connection factory.");
        log.info("    -uniquedests                - Each consumer thread uses a unique destination.");
        log.info("    -help-ssl                   - Print help on SSL parameters.");
    }
    
}

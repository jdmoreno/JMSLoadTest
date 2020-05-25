package eps.platform.tools.jms;

import javax.jms.DeliveryMode;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder(buildMethodName = "build", toBuilder = true, access = AccessLevel.PUBLIC, setterPrefix = "set")
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class JMSProducerParams {
	
	@Setter @Getter @Builder.Default private boolean initialised = false;
	
    // parameters
	@Getter private @Builder.Default String serverUrl = "tcp://localhost:7222";
	@Getter private @Builder.Default String username = null;
	@Getter private @Builder.Default String password = null;
	@Getter private @Builder.Default int    connections = 1;
	@Getter private @Builder.Default String destName = "topic.sample";
	@Getter private @Builder.Default String factoryName = null;

	@Getter private @Builder.Default boolean useTopic = true;
	@Getter private @Builder.Default boolean uniqueDests = false;
	@Getter private @Builder.Default boolean xa = false;
		
    @Getter private @Builder.Default String payloadFile = null;
    @Getter private @Builder.Default boolean compression = false;
    @Getter private @Builder.Default int msgRate = 0;
    @Getter private @Builder.Default int txnSize = 0;
    @Getter private @Builder.Default int count = 10000;
    @Getter private @Builder.Default int runTime = 0;
    @Getter private @Builder.Default int msgSize = 0;
    @Getter private @Builder.Default int threads = 1;
    @Getter private @Builder.Default int deliveryMode = DeliveryMode.NON_PERSISTENT;
    @Getter private @Builder.Default boolean async = false;
    
    public JMSProducerParams(String[] args)
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
     * Print the usage and exit.
     */
    public void usage()
    {
        log.info("\nUsage: java tibjmsMsgProducerPerf [options] [ssl options]");
        log.info("   where options are:");
        log.info("   -server       <server URL>  - EMS server URL, default is local server");
        log.info("   -user         <user name>   - user name, default is null");
        log.info("   -password     <password>    - password, default is null");
        log.info("   -topic        <topic-name>  - topic name, default is \"topic.sample\"");
        log.info("   -queue        <queue-name>  - queue name, no default");
        log.info("   -size         <nnnn>        - Message payload in bytes");
        log.info("   -count        <nnnn>        - Number of messages to send, default 10k");
        log.info("   -time         <seconds>     - Number of seconds to run");
        log.info("   -threads      <nnnn>        - Number of threads to use for sends");
        log.info("   -connections  <nnnn>        - Number of connections to use for sends");
        log.info("   -delivery     <nnnn>        - DeliveryMode, default NON_PERSISTENT");
        log.info("   -txnsize      <count>       - Number of messages per transaction");
        log.info("   -rate         <msg/sec>     - Message rate for each producer thread");
        log.info("   -payload      <file name>   - File containing message payload.");
        log.info("   -factory      <lookup name> - Lookup name for connection factory.");
        log.info("   -uniquedests                - Each producer uses a different destination");
        log.info("   -compression                - Enable compression while sending msgs ");
        log.info("   -async                      - Use asynchronous sends");
        log.info("   -xa                         - Use XA transactions ");
        log.info("   -help-ssl                   - help on ssl parameters\n");
        
//      System.exit(0);
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
                if ((i+1) >= args.length) { return false; }
                serverUrl = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-queue")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                destName = args[i+1];
                i += 2;
                useTopic = false;
            }
            else if (args[i].compareTo("-topic")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                destName = args[i+1];
                i += 2;
                useTopic = true;
            }
            else if (args[i].compareTo("-user")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                username = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-password")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                password = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-delivery")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                String dm = args[i+1];
                i += 2;
                try {
                	deliveryMode = JMSPerfCommon.deliveryModeValue(dm);
                } catch (IllegalArgumentException e) {
                	return false;
	    		}
            }
            else if (args[i].compareTo("-count")==0)
            {
            	if ((i+1) >= args.length) { return false; }
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
            	if ((i+1) >= args.length) { return false; }
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
            else if (args[i].compareTo("-threads")==0)
            {
            	if ((i+1) >= args.length) { return false; }
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
            	if ((i+1) >= args.length) { return false; }
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
            else if (args[i].compareTo("-size")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                try 
                {
                    msgSize = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e) 
                {
                	log.error("Error: invalid value of -size parameter");
                	return false;
                }
                i += 2;
            }
            else if (args[i].compareTo("-txnsize")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                try 
                {
                    txnSize = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e) 
                {
                	log.error("Error: invalid value of -txnsize parameter");
                	return false;
//                    usage();
                }
                if (txnSize < 1) 
                {
                	log.error("Error: invalid value of -txnsize parameter");
                	return false;
//                    usage();
                }
                i += 2;
            }
            else if (args[i].compareTo("-rate")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                try 
                {
                    msgRate = Integer.parseInt(args[i+1]);
                }
                catch (NumberFormatException e)
                {
                	log.error("Error: invalid value of -rate parameter");
                	return false;
//                    usage();
                }
                if (msgRate < 1)
                {
                	log.error("Error: invalid value of -rate parameter");
                	return false;
//                    usage();
                }
                i += 2;
            }
            else if (args[i].compareTo("-payload")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                payloadFile = args[i+1];
                i += 2;
            }
            else if (args[i].compareTo("-factory")==0)
            {
            	if ((i+1) >= args.length) { return false; }
                factoryName = args[i+1];
                i += 2;
            }
            else if (args[i].startsWith("-ssl"))
            {
                i += 2;
            }
            else if (args[i].compareTo("-uniquedests")==0)
            {
                uniqueDests = true;
                i += 1;
            }
            else if (args[i].compareTo("-compression")==0)
            {
                compression = true;
                i += 1;
            } 
            else if (args[i].compareTo("-async")==0)
            {
                async = true;
                i += 1;
            } 
            else if (args[i].compareTo("-xa")==0)
            {
                xa = true;
                i += 1;
            } 
            else if (args[i].compareTo("-help")==0)
            {
            	return false;
            }
            else if (args[i].compareTo("-help-ssl")==0)
            {
            	SSLParams.usage();
            	return false;
            }
            else
            {
            	log.error("Error: invalid option: " + args[i]);
            	return false;
            }
        }
        return true;
    }
    
}

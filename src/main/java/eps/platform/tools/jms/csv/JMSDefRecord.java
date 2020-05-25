package eps.platform.tools.jms.csv;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder(buildMethodName = "build", toBuilder = true, access = AccessLevel.PUBLIC, setterPrefix = "set")
@AllArgsConstructor()
@ToString
public class JMSDefRecord {
	
	@Getter private String serverUrl;
	@Getter private String username;
	@Getter private String password;
	@Getter private int connections;
	@Getter private String factoryName;
	@Getter private boolean isTopic;
	@Getter private String destName;
	@Getter private boolean useUniqueDests;
	@Getter private boolean useXA;
	@Getter private int txnSize;
	@Getter private boolean useProducer;
	@Getter private int countProducer;
	@Getter private int runTime;
	@Getter private int msgSize;
	@Getter private int threadsProducer;
	@Getter private String payloadFile;
	@Getter private boolean useCompression;
	@Getter private int msgRate;
	@Getter private boolean useConsumer;
	@Getter private int countConsumer;
	@Getter private int threadsConsumer;	
	@Getter private int deliveryMode;
	@Getter private boolean useAsync;
	@Getter private String selector;
	@Getter private int ackMode;

}

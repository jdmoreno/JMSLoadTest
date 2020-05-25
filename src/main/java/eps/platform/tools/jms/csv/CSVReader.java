package eps.platform.tools.jms.csv;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import eps.platform.tools.jms.JMSConsumer;
import eps.platform.tools.jms.JMSConsumerParams;
import eps.platform.tools.jms.JMSPerfCommon;
import eps.platform.tools.jms.JMSProducer;
import eps.platform.tools.jms.JMSProducerParams;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CSVReader {
	static List<JMSDefRecord> jmsDefRecords = new ArrayList<>();
	private CSVReader() {
		for (JMSDefRecord jmsDefRecord : jmsDefRecords) {
			if (jmsDefRecord.isUseProducer()) {
				Launcher launcher = new Launcher(jmsDefRecord, true);
				Thread thread = new Thread(launcher);
				thread.start();
			}
			if (jmsDefRecord.isUseConsumer()) {
				Launcher launcher = new Launcher(jmsDefRecord, false);
				Thread thread = new Thread(launcher);
				thread.start();
			}			
		}
	}
	
    public static void main(String[] args)
    {
    	Reader in;
    	String filePath = args[0];
    	log.info(filePath);
		try {
			in = new FileReader(filePath);
	    	Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in);
	    	for (CSVRecord record : records) {
	    		try {
	    			log.info(record.toString());
	    			readRecord(record);
	    		} catch (IllegalArgumentException e) {
	    			log.error("Skipping CSV row");
	    		}
	    	}
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		new CSVReader();
	}

	private static void readRecord(CSVRecord record) {
		String columnName;
		String paramName;
		// server. Optional
		columnName = "serverUrl";
		paramName = "server";
		String serverUrl = getValue(record, columnName, paramName, String.class, (String) "tcp://localhost:7222");
		
		// user. Optional
		columnName = "username";
		paramName = "user";
		String username = getValue(record, columnName, paramName, String.class, (String) null);
		
		// password. Optional
		columnName = "password";
		paramName = "password";
		String password = getValue(record, columnName, paramName, String.class, (String) null);
		
		// connections. Optional. integer, 0 default
		columnName = "connections";
		paramName = "connections";
		int connections = getValue(record, columnName, paramName, Integer.class, (Integer) 1);
		
		// factoryName. Optional
		columnName = "factoryName";
		paramName = "factoryName";
		String factoryName = getValue(record, columnName, paramName, String.class, (String) null);
		
		// isTopic. Optional. false default
		columnName = "isTopic";
		paramName = "isTopic";
		boolean isTopic = getValue(record, columnName, paramName, Boolean.class, (Boolean) false);
		
		// destName. Optional. empty default
		columnName = "destName";
		paramName = "destName";
		String destName = getValue(record, columnName, paramName, String.class, (String) null);
		
		// uniqueDests. Optional. false default
		columnName = "UseUniqueDests";
		paramName = "uniqueDests";
		boolean useUniqueDests = getValue(record, columnName, paramName, Boolean.class, (Boolean) false);
		
		// xa. Optional. false default
		columnName = "UseXA";
		paramName = "xa";
		boolean useXA = getValue(record, columnName, paramName, Boolean.class, (Boolean) false);
		
		// txnSize. Optional. integer, 0 default
		columnName = "txnSize";
		paramName = "txnSize";
		int txnSize = getValue(record, columnName, paramName, Integer.class, (Integer) 0);

		// time. Optional. 100 default
		columnName = "RunTime";
		paramName = "time";
		int runTime = getValue(record, columnName, paramName, Integer.class, (Integer) 100);
		
		// xa. Optional. false default
		columnName = "UseProducer";
		paramName = "UseProducer";
		boolean useProducer = getValue(record, columnName, paramName, Boolean.class, (Boolean) false);
		
		// count. Optional. 10000 default
		columnName = "countProducer";
		paramName = "countProducer";
		int countProducer = getValue(record, columnName, paramName, Integer.class, (Integer) 10000);

		// threadsProducer. Optional. 1 default
		columnName = "threadsProducer";
		paramName = "threadsProducer";
		int threadsProducer = getValue(record, columnName, paramName, Integer.class, (Integer) 1);

		// msgRate. Optional. 20 default
		columnName = "msgRate";
		paramName = "msgRate";
		int msgRate = getValue(record, columnName, paramName, Integer.class, (Integer) 20);
		
		// payloadFile. Optional. empty default
		columnName = "payloadFile";
		paramName = "payloadFile";
		String payloadFile = getValue(record, columnName, paramName, String.class, (String) null);

		// compression. Optional. false default
		columnName = "UseCompression";
		paramName = "compression";
		boolean useCompression = getValue(record, columnName, paramName, Boolean.class, (Boolean) false);

		// msgSize. Optional. 20 default
		columnName = "msgSize";
		paramName = "msgSize";
		int msgSize = getValue(record, columnName, paramName, Integer.class, (Integer) 20);

		// xa. Optional. false default
		columnName = "UseConsumer";
		paramName = "UseConsumer";
		boolean useConsumer = getValue(record, columnName, paramName, Boolean.class, (Boolean) false);
		
		// count. Optional. 10000 default
		columnName = "countConsumer";
		paramName = "countConsumer";
		int countConsumer = getValue(record, columnName, paramName, Integer.class, (Integer) 10000);
		
		// threadsConsumer. Optional. 1 default
		columnName = "threadsConsumer";
		paramName = "threadsConsumer";
		int threadsConsumer = getValue(record, columnName, paramName, Integer.class, (Integer) 1);
		
		// deliveryMode. Optional. empty default
		columnName = "deliveryMode";
		paramName = "deliveryMode";
		String deliveryModeStr = getValue(record, columnName, paramName, String.class, (String) "PERSISTENT");
		int deliveryMode = JMSPerfCommon.deliveryModeValue(deliveryModeStr);

		// async. Optional. false default
		columnName = "UseAsync";
		paramName = "async";
		boolean useAsync = getValue(record, columnName, paramName, Boolean.class, (Boolean) false);

		// selector. Optional. empty default
		columnName = "selector";
		paramName = "selector";
		String selector = getValue(record, columnName, paramName, String.class, (String) null);

		// ackMode. Optional. empty default
		columnName = "ackMode";
		paramName = "ackMode";
		String ackModeStr = getValue(record, columnName, paramName, String.class, (String) "AUTO");
		int ackMode = 0;
        ackMode = JMSPerfCommon.ackModeValue(ackModeStr);
		
		JMSDefRecord jmsDefRecord = JMSDefRecord.builder()
			.setServerUrl(serverUrl)
			.setUsername(username)
			.setPassword(password)
			.setConnections(connections)
			.setFactoryName(factoryName)
			.setIsTopic(isTopic)
			.setDestName(destName)
			.setUseUniqueDests(useUniqueDests)
			.setUseXA(useXA)
			.setTxnSize(txnSize)
			.setUseProducer(useProducer)
			.setCountProducer(countProducer)
			.setRunTime(runTime)
			.setThreadsProducer(threadsProducer)
			.setPayloadFile(payloadFile)
			.setUseCompression(useCompression)
			.setMsgRate(msgRate)
			.setMsgSize(msgSize)
			.setUseConsumer(useConsumer)
			.setCountConsumer(countConsumer)
			.setThreadsConsumer(threadsConsumer)
			.setDeliveryMode(deliveryMode)
			.setUseAsync(useAsync)
			.setSelector(selector)
			.setAckMode(ackMode).build();
		log.info(jmsDefRecord.toString());
		jmsDefRecords.add(jmsDefRecord);
		
//		if (jmsDefRecord.isUseProducer()) {
//			Launcher launcher = new Launcher(jmsDefRecord);
//			Thread thread = new Thread(launcher);
//			thread.start();
//		}
		
//		if (jmsDefRecord.isUseConsumer()) {
//			JMSConsumerParams jmsConsumerParams = JMSConsumerParams.builder()
//				.setServerUrl(serverUrl)
//				.setUsername(username)
//				.setPassword(password)
//				.setConnections(connections)
//				.setDestName(destName)
//				.setFactoryName(factoryName)
//				.setUseTopic(isTopic)
//				.setUniqueDests(useUniqueDests)
//				.setXa(useXA)
//				.setSelector(selector)
//				.setTxnSize(txnSize)
//				.setCount(countConsumer)
//				.setRunTime(runTime)
//				.setThreads(threadsConsumer)
//				.setAckMode(ackMode)
//				.build();
//			log.info(jmsConsumerParams.toString());	
//			jmsConsumerParams.initialise();
//			new JMSConsumer(jmsConsumerParams, null);
//		}
	}

	private static Integer getValue(CSVRecord record, String columnName, String paramName, Class<Integer> type, Integer defValue) {
	    if (record.isSet(columnName)) {
	    	String columnValue = record.get(columnName);			
	    	try 
            {
				if (StringUtils.isEmpty(columnValue)) {
					return defValue;
				} else {
					return Integer.parseInt(columnValue);
				}
            }
            catch (NumberFormatException e)
            {
            	log.error("Error: invalid value {} for -{} parameter", columnValue, paramName);
            	throw new IllegalArgumentException("Bad type.");
            }
	    } else {
	    	return defValue;
	    }
	}

	private static Boolean getValue(CSVRecord record, String columnName, String paramName, Class<Boolean> type, Boolean defValue) {
	    if (record.isSet(columnName)) {
	    	String columnValue = record.get(columnName);			
	    	try 
            {
				if (StringUtils.isEmpty(columnValue)) {
					return defValue;
				} else {
					return Boolean.valueOf(columnValue);
				}
            }
            catch (NumberFormatException e)
            {
            	log.error("Error: invalid value {} for -{} parameter", columnValue, paramName);
            	throw new IllegalArgumentException("Bad type.");
            }
	    } else {
	    	return defValue;
	    }
	}
	
	private static String getValue(CSVRecord record, String columnName, String paramName, Class<String> type, String defValue) {
		String columnValue = null;
	    if (record.isSet(columnName)) {
	    	columnValue = record.get(columnName);
			if (StringUtils.isEmpty(columnValue)) {
				return defValue;
			} else {
				return columnValue;
			}
	    } else {
	    	return defValue;
	    }
	}
	
	class Launcher implements Runnable  {
		private boolean isProducer;
		private JMSDefRecord jmsDefRecord;
		
		public Launcher(JMSDefRecord jmsDefRecord, boolean isProducer) {
			this.jmsDefRecord = jmsDefRecord;
			this.isProducer = isProducer;
		}
		
		@Override
		public void run() {
			if (isProducer) {
				if (jmsDefRecord.isUseProducer()) {
					JMSProducerParams jmsProducerParams = JMSProducerParams.builder()
						.setServerUrl(jmsDefRecord.getServerUrl())
						.setUsername(jmsDefRecord.getUsername())
						.setPassword(jmsDefRecord.getPassword())
						.setConnections(jmsDefRecord.getConnections())
						.setDestName(jmsDefRecord.getDestName())
						.setFactoryName(jmsDefRecord.getFactoryName())
						.setUseTopic(jmsDefRecord.isTopic())
						.setUniqueDests(jmsDefRecord.isUseUniqueDests())
						.setXa(jmsDefRecord.isUseXA())
						.setRunTime(jmsDefRecord.getRunTime())
						.setThreads(jmsDefRecord.getThreadsProducer())
						.setCount(jmsDefRecord.getCountProducer())
						.setPayloadFile(jmsDefRecord.getPayloadFile())
						.setCompression(jmsDefRecord.isUseCompression())
						.setMsgRate(jmsDefRecord.getMsgRate())
						.setTxnSize(jmsDefRecord.getTxnSize())
						.setMsgSize(jmsDefRecord.getMsgSize())
						.setDeliveryMode(jmsDefRecord.getDeliveryMode())
						.setAsync(jmsDefRecord.isUseAsync())
						.build();
					log.info(jmsProducerParams.toString());		
					jmsProducerParams.initialise();
					new JMSProducer(jmsProducerParams, null);
				}
			} else {
				if (jmsDefRecord.isUseConsumer()) {
					JMSConsumerParams jmsConsumerParams = JMSConsumerParams.builder()
						.setServerUrl(jmsDefRecord.getServerUrl())
						.setUsername(jmsDefRecord.getUsername())
						.setPassword(jmsDefRecord.getPassword())
						.setConnections(jmsDefRecord.getConnections())
						.setDestName(jmsDefRecord.getDestName())
						.setFactoryName(jmsDefRecord.getFactoryName())
						.setUseTopic(jmsDefRecord.isTopic())
						.setUniqueDests(jmsDefRecord.isUseUniqueDests())
						.setXa(jmsDefRecord.isUseXA())
						.setSelector(jmsDefRecord.getSelector())
						.setTxnSize(jmsDefRecord.getTxnSize())
						.setCount(jmsDefRecord.getCountConsumer())
						.setRunTime(jmsDefRecord.getRunTime())
						.setThreads(jmsDefRecord.getThreadsConsumer())
						.setAckMode(jmsDefRecord.getAckMode())
						.build();
					log.info(jmsConsumerParams.toString());	
					jmsConsumerParams.initialise();
					new JMSConsumer(jmsConsumerParams, null);
				}
			}
		}
	}
}

package eps.platform.tools.jms;

public class MsgRateChecker {
	  private int msgRate;
      private long sampleStart;
      private int sampleTime;
      private int sampleCount;
      
      MsgRateChecker(int msgRate)
      {
    	  this.msgRate = msgRate;
          this.sampleTime = 10;
      }
      
      void checkMsgRate(int count)
      {
          if (msgRate < 100)
          {
              if (count % 10 == 0)
              {
                 try
                 {
                     long sleepTime = (long)((10.0/(double)msgRate)*1000);
                     Thread.sleep(sleepTime);
                 }
                 catch (InterruptedException e) {}
              }
          }
          else if (sampleStart == 0)
          {
              sampleStart = System.currentTimeMillis();
          }
          else
          {
              long elapsed = System.currentTimeMillis() - sampleStart;
              if (elapsed >= sampleTime)
              {
                  int actualMsgs = count - sampleCount;
                  int expectedMsgs = (int)(elapsed*((double)msgRate/1000.0));
                  if (actualMsgs > expectedMsgs)
                  {
                      long sleepTime = (long)((double)(actualMsgs-expectedMsgs)/((double)msgRate/1000.0));
                      try 
                      {
                          Thread.sleep(sleepTime);
                      }
                      catch (InterruptedException e) {}

                      if (sampleTime > 20)
                          sampleTime -= 10;
                  }
                  else
                  {
                      if (sampleTime < 300)
                          sampleTime += 10;
                  }
                  sampleStart = System.currentTimeMillis();
                  sampleCount = count;
              }
          }
      }
}

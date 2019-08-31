package ribot.amandine.kafka.app.runnable;

import org.apache.http.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ribot.amandine.kafka.app.Disruption;
import ribot.amandine.kafka.app.client.SncfRESTClient;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class SncfRestThread implements Runnable {

    private Logger log = LoggerFactory.getLogger(SncfRestThread.class.getSimpleName());

    private final ArrayBlockingQueue<Disruption> disruptionsQueue;
    private final CountDownLatch latch;
    private SncfRESTClient sncfRESTClient;
    private Integer startPage = 4;

    public SncfRestThread(AppConfig appConfig, ArrayBlockingQueue<Disruption> disruptionsQueue, CountDownLatch latch) {
        this.disruptionsQueue = disruptionsQueue;
        this.latch = latch;
        sncfRESTClient = new SncfRESTClient(appConfig);
    }

    @Override
    public void run() {
        try {
            Boolean keepOnRunning = true;
            while (keepOnRunning){
                List<Disruption> disruptions;

                try {

                    disruptions = sncfRESTClient.getNextDisruptions(startPage);
                    startPage -= 1;

                    System.out.println("Fetched " + disruptions.size() + " disruptions");

                    if(startPage < 0){
                        System.out.println("Sleep for 2 min before next fetch");
                        Thread.sleep(120000);
                        startPage = 4;
                    }
                    else {
                        // this may block if the queue is full - this is flow control
                        //System.out.println("Queue size :" + disruptionsQueue.size());
                        for (Disruption disruption : disruptions){
                            disruptionsQueue.put(disruption);
                        }
                    }
                } catch (HttpException e) {
                    e.printStackTrace();
                    Thread.sleep(500);
                } finally {
                    Thread.sleep(50);
                }
            }
        } catch (InterruptedException e) {
            System.out.println("REST Client interrupted");
        } finally {
            this.close();
        }
    }


    private void close() {
        log.info("Closing");
        sncfRESTClient.close();
        latch.countDown();
        log.info("Closed");
    }
}

package ribot.amandine.kafka.app.runnable;

import org.apache.http.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ribot.amandine.kafka.app.Disruption;
import ribot.amandine.kafka.app.client.SncfRESTClient;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class SncfRestThread implements Runnable {

    private Logger log = LoggerFactory.getLogger(SncfRestThread.class.getSimpleName());

    private final ArrayBlockingQueue<Disruption> disruptionsQueue;
    private final CountDownLatch latch;
    private SncfRESTClient sncfRESTClient;
    private String dateNowFormatted;
    private String dateMinusMinuteFormatted;
    private DateFormat dateFormatter;
    private Date dateNow;
    private Date dateMinusOneMinute;

    public SncfRestThread(AppConfig appConfig, ArrayBlockingQueue<Disruption> disruptionsQueue, CountDownLatch latch) {
        this.disruptionsQueue = disruptionsQueue;
        this.latch = latch;
        sncfRESTClient = new SncfRESTClient(appConfig);

        dateFormatter = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
        dateNow = new Date();
        dateMinusOneMinute = new Date(System.currentTimeMillis() - 60000);
        dateNowFormatted = dateFormatter.format(dateNow);
        dateMinusMinuteFormatted = dateFormatter.format(dateMinusOneMinute);
    }

    @Override
    public void run() {
        try {
            Boolean keepOnRunning = true;
            while (keepOnRunning) {
                List<Disruption> disruptions;

                try {

                    disruptions = sncfRESTClient.getNextDisruptions(dateNowFormatted, dateMinusMinuteFormatted);

                    System.out.println("Fetched " + disruptions.size() + " disruptions");
                    System.out.println("Sleep for 1 min before next fetch");

                    Thread.sleep(120000);
                    dateMinusOneMinute = dateNow;
                    dateNow = new Date();
                    dateMinusMinuteFormatted = dateNowFormatted;
                    dateNowFormatted = dateFormatter.format(dateNow);

                    // this may block if the queue is full - this is flow control
                    //System.out.println("Queue size :" + disruptionsQueue.size());
                    for (Disruption disruption : disruptions) {
                        disruptionsQueue.put(disruption);
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

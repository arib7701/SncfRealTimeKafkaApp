package ribot.amandine.kafka.app;

import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ribot.amandine.kafka.app.configuration.AppConfig;
import ribot.amandine.kafka.app.runnable.SncfAvroProducerThread;
import ribot.amandine.kafka.app.runnable.SncfRestThread;

import java.util.List;
import java.util.concurrent.*;

public class SncfApiProducer {

    private Logger log = LoggerFactory.getLogger(SncfApiProducer.class.getSimpleName());

    // thread safe queue which blocks when full.
    private ExecutorService executor;
    private CountDownLatch latch;
    private SncfRestThread sncfRESTClient;
    private SncfAvroProducerThread sncfProducer;

    public static void main(String[] args) {

        SncfApiProducer app = new SncfApiProducer();
        app.start();
    }

    private SncfApiProducer() {

        AppConfig appConfig = new AppConfig(ConfigFactory.load());
        latch = new CountDownLatch(2);
        executor = Executors.newFixedThreadPool(2);
        ArrayBlockingQueue<Train> trainsQueue = new ArrayBlockingQueue<>(appConfig.getQueueCapacity());
        sncfRESTClient = new SncfRestThread(appConfig, trainsQueue, latch);
        sncfProducer = new SncfAvroProducerThread(appConfig, trainsQueue, latch);
    }

    private void start() {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!executor.isShutdown()){
                log.info("Shutdown requested");
                shutdown();
            }
        }));

        log.info("Application started!");
        executor.submit(sncfRESTClient);
        executor.submit(sncfProducer);
        log.info("Stuff submit");
        try {
            log.info("Latch await");
            latch.await();
            log.info("Threads completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            shutdown();
            log.info("Application closed succesfully");
        }
    }

    private void shutdown() {
        if (!executor.isShutdown()) {
            log.info("Shutting down");
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(2000, TimeUnit.MILLISECONDS)) { //optional *
                    log.warn("Executor did not terminate in the specified time."); //optional *
                    List<Runnable> droppedTasks = executor.shutdownNow(); //optional **
                    log.warn("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed."); //optional **
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

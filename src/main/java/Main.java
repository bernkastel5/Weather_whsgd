public class Main {
    public static void main(String[] args) {
        Thread consumerThread = new Thread(() -> new WeatherConsumer().consume());
        consumerThread.start();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        new WeatherProducer().produce();
    }
}

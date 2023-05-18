import java.io.File;

public class TestHarness {

  public static void main(String[] args) {
    try {
      // Start the controller
      System.out.println("Starting Controller...");
      Thread controllerThread = new Thread(() -> Con.main(new String[0]));
      controllerThread.start();

      // Wait for the controller to start
      Thread.sleep(1000);

      // Start the DStores
      System.out.println("Starting DStores...");
      Thread dstoreThread1 = new Thread(() -> ds.main(new String[]{"5000", "4000", "11000", "DStore1"}));
      Thread dstoreThread2 = new Thread(() -> ds2.main(new String[]{"5001", "4000", "11000", "DStore2"}));
      Thread dstoreThread3 = new Thread(() -> ds3.main(new String[]{"5002", "4000", "11000", "DStore3"}));
      Thread dstoreThread4 = new Thread(() -> ds4.main(new String[]{"5003", "4000", "11000", "DStore4"}));

      dstoreThread1.start();
      dstoreThread2.start();
      dstoreThread3.start();
      dstoreThread4.start();

      // Wait for the DStores to start
      Thread.sleep(1000);

      // Start the client
      System.out.println("Starting Client...");
      Thread clientThread = new Thread(() -> {
        try {
          File uploadFolder = new File("to_store");
          if (!uploadFolder.exists())
            throw new RuntimeException("to_store folder does not exist");

          ClientApp clientApp = new ClientApp(4000, 1000, new File("downloads"), uploadFolder);
          clientApp.start();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      clientThread.start();

      // Wait for the client to start
      Thread.sleep(1000);

      // Send commands to the client
      sendCommand("STORE 1.jpeg 20", clientThread);
      sendCommand("LIST", clientThread);
      sendCommand("STORE 2.jpg 20", clientThread);
      sendCommand("LIST", clientThread);
      sendCommand("STORE doc.docx 20", clientThread);
      sendCommand("LIST", clientThread);
      sendCommand("STORE spec.pdf", clientThread);
      sendCommand("LIST", clientThread);
      sendCommand("LOAD 1.jpeg", clientThread);
      sendCommand("REMOVE 1.jpeg", clientThread);
      sendCommand("LIST", clientThread);
      sendCommand("REMOVE 2.jpg", clientThread);
      sendCommand("LIST", clientThread);
      sendCommand("REMOVE spec.pdf", clientThread);
      sendCommand("LIST", clientThread);
      sendCommand("REMOVE doc.docx", clientThread);
      sendCommand("LIST", clientThread);
      sendCommand("QUIT", clientThread);

      // Wait for the client to finish
      clientThread.join();

      // Stop the DStores
      System.out.println("Stopping DStores...");
      dstoreThread1.interrupt();
      dstoreThread2.interrupt();
      dstoreThread3.interrupt();
      dstoreThread4.interrupt();

      // Stop the controller
      System.out.println("Stopping Controller...");
      controllerThread.interrupt();

      System.out.println("Test completed.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void sendCommand(String command, Thread clientThread) throws InterruptedException {
    Thread.sleep(1000);
    System.out.println("Sending command: " + command);
    synchronized (clientThread) {
      System.setIn(new java.io.ByteArrayInputStream(command.getBytes()));
      clientThread.notify();
    }
  }
}

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.Timer;
import java.util.TimerTask;

public class Controller {

  static ArrayList arrayList;
  static HashMap hashMap;
  static Index index = new Index(arrayList, hashMap);
  static ArrayList<Integer> dStores = new ArrayList();
  static HashMap<String, ArrayList<Integer>> filesList = new HashMap<>();

  static HashMap<String, PrintWriter> storePW = new HashMap<>();
  static HashMap<String, PrintWriter> rmvPW = new HashMap<>();

  static int rebalanceCounter = 0;

  static int rebalanceCompleteCounter = 0;

  static boolean rebalancing = false;

  static int rep_factor = 0;

  static int completed_stores = 0;

  static int completed_deletes = 0;

  static int loadPortCounter = 0;

  static int loadPort = 0;

  /**
   * Main method for Controller
   * @param args
   */
  public static void main(String[] args) {

    if (args.length != 4) {
      System.out.println("Invalid number of arguments");
      return;
    }

    final int cport = Integer.parseInt(args[0]);
    final int rep_factor = Integer.parseInt(args[1]);
    final int timeout = Integer.parseInt(args[2]);
    final int rebalance_period = Integer.parseInt(args[3]);

  }

  /**
   * Check if there is sufficient number of dstores connected to the controller
   */
  private synchronized static boolean sufficientDS() {
    return dStores.size() >= rep_factor;
  }

  /**
   * Add a DStore to the list of dstores
   * @param port
   */
  private synchronized void addDS(int port) {
    if (!dStores.contains(port)) {
      try {
        dStores.add(port);
        System.out.println("DStore [" + port + "] added, " + dStores.size() + " DStores connected.");
        if (sufficientDS()) {
          notifyAll();
        }
      } catch (Exception e) {
        System.out.println("Error adding DStore [" + port + "]" + dStores.size() + " DStores connected.");
      }
    }
  }

  /**
   * List all the files in dstores
   */
  private synchronized static void listFiles() {
    if (index.getFiles().size()>0) {
      System.out.println("Listing files...");
      for (String file : index.getFiles()) {
        System.out.println(">> " + file );
      }
    } else {
      System.out.println("No files to list.");
    }
  }


  /**
   * storeFile method for Controller
   * This method is called when the user enters the STORE command
   * It checks if the file already exists in the system
   * If it does not exist, it stores the file in the system
   * If it does exist, it prints an error message
   * @param printWriter
   * @param rep_factor
   * @param fileName
   * @param size
   */
  private synchronized static void storeFile (PrintWriter printWriter, int rep_factor, String fileName, int size){
    if (index.getFiles().contains(fileName)) {
      System.out.println("File already exists.");
      printWriter.println("ERROR_FILE_ALREADY_EXISTS");
      return;
    } else {
      ArrayList<Integer> samp = new ArrayList<>();
      samp.add(size);
      filesList.put(fileName, samp);
      index.fileStats.put(fileName, "STORING");
      System.out.println("Storing file " + fileName + "...");
      String ports = "";
      for (int i = 0; i < rep_factor; i++) {
        ports = ports + " " + dStores.get(i);
      }
      printWriter.println("STORE_TO " + ports);
      System.out.println("STORE_TO command sent to DStores at port " + ports);
    }
  }

  /**
   * storeComplete method for Controller
   * This method is called when the user enters the STORE_COMPLETE command
   * It checks if the file has been stored in the required number of dstores
   * If it has, it prints a message and sends the STORE_COMPLETE command to the dstores
   * If it has not, it increments the completed_stores counter
   * @param port
   * @param printWriter
   * @param completed_stores
   * @param fileName
   */
  private synchronized void storeComplete(int port, PrintWriter printWriter, int completed_stores, String fileName){
    if (completed_stores == rep_factor) {
      System.out.println("File: " + fileName + " stored into DStores at ports " + dStores.toString() + "...");
      index.files.add(fileName);
      //setCompleted_stores(0);

      if(filesList.get(fileName).contains(port)){
        System.out.println("Error: File already exists in DStore at port " + port);
        //should not happen under normal circumstances
      } else {
        filesList.get(fileName).add(port);
        filesList.put(fileName, filesList.get(fileName));
      }

      index.fileStats.remove(fileName);
      index.fileStats.put(fileName, "STORE_COMPLETE");
      printWriter.println("STORE_COMPLETE");
      System.out.println("STORE_COMPLETE command sent to DStore at port [" + port + "]");
    } else {
      //setCompleted_stores(getCompleted_stores()+1);
      if (filesList.get(fileName).contains(port)==false){
        filesList.get(fileName).add(port);
        filesList.put(fileName, filesList.get(fileName));
      }
    }
  }

  /**
   * loadFile method for Controller
   * This method is called when the user enters the LOAD command
   * It checks if the file exists in the system
   * If it does not exist, it prints an error message
   * If it does exist, it loads the file from the system
   * @param printWriter
   * @param fileName
   * @param loadPortCounter
   */
  private synchronized static void loadFile (PrintWriter printWriter, String fileName, int loadPortCounter){
    for (String key : filesList.keySet()){
      if (key.equals(fileName) && index.files.contains(key)){
        int file_size = filesList.get(fileName).get(0);
        int port = filesList.get(fileName).get(loadPortCounter);
        System.out.println("Loading size " + file_size + " file from DStore at port " + port + "...");
        printWriter.println("LOAD_FROM " + port + " " + file_size);
      }
    }
  }


  /**
   * removeFile method for Controller
   * This method is called when the user enters the REMOVE command
   * It checks if the file exists in the system
   * If it does not exist, it prints an error message
   * If it does exist, it removes the file from the system
   * @param printWriter
   * @param fileName
   */
  private synchronized static void removeFile(PrintWriter printWriter, String fileName){
    if (!index.files.contains(fileName)) {
      System.out.println("File " + fileName + " does not exist.");
      printWriter.println("ERROR_FILE_DOES_NOT_EXIST");
    } else {
      index.files.remove(fileName);
      index.fileStats.put(fileName, "DELETING");
      System.out.println("Deleting file " + fileName + "...");
      if (filesList.containsKey(fileName)){
        ArrayList<Integer> ports = filesList.get(fileName);
        for (int i = 0; i < ports.size(); i++){
          try {
            removeCommand(ports.get(i), fileName);
          } catch (IOException e) {
            System.out.println("Error sending REMOVE signal to DStore at port " + ports.get(i));
          }
        }
      }
    }
  }

  /**
   * removeCommand method for Controller
   * This method is called when the user enters the REMOVE command
   * It sends the REMOVE command to the dstores
   * @param port
   * @param fileName
   * @throws IOException
   */
  private synchronized static void removeCommand(Integer port, String fileName) throws IOException {
      Socket dStores = new Socket(InetAddress.getLoopbackAddress(), port);
      PrintWriter dStorePW = new PrintWriter(dStores.getOutputStream(), true);
      dStorePW.println("REMOVE " + fileName);
  }

  /**
   * removeComplete helper method for Controller
   * It checks if the file has been deleted from the required number of dstores
   * If it has, it prints a message and sends the REMOVE_COMPLETE command to the dstores
   * If it has not, it increments the completed_deletes counter
   * @param port
   * @param printWriter
   * @param completed_deletes
   * @param fileName
   */
  private synchronized static void removeComplete(int port, PrintWriter printWriter, int completed_deletes, String fileName){
    if (completed_deletes == rep_factor) {
      System.out.println("File: " + fileName + " deleted from DStores at ports " + dStores.toString() + "...");
      //setCompleted_deletes(0);

      filesList.remove(fileName);

      index.fileStats.remove(fileName);
      index.files.remove(fileName);
      printWriter.println("REMOVE_COMPLETE");
      System.out.println("REMOVE_COMPLETE command sent to DStore at port [" + port + "]");
    } else {
      //setCompleted_deletes(getCompleted_deletes()+1);
      filesList.get(fileName).remove(port);
    }
  }

  /**
   * rebalanceTimer method for Controller
   * Automatically rebalances the system every reb_period
   * @param reb_period
   */
  private synchronized void rebalanceTimer(int reb_period){
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        rebalance();
      }
    }, reb_period, reb_period);
  }

  private synchronized static void rebalance(){
    mapRebalance();
    if(dStores.size() > 0){
      rebalancing = true;
    }
    for(int i = 0; i < dStores.size(); i++){
      try {
        Socket dStore = new Socket(InetAddress.getLoopbackAddress(), dStores.get(i));
        PrintWriter dStorePW = new PrintWriter(dStore.getOutputStream(), true);
        dStorePW.println("LIST");
        System.out.println("LIST command sent to DStore at port " + dStores.get(i));
      } catch (IOException e) {
        System.out.println("IO Error sending LIST signal to DStore at port " + dStores.get(i));
      } catch (Exception e){
        System.out.println("Error sending LIST signal to DStore at port " + dStores.get(i));
      }
      System.out.println("LIST command sent to DStores at ports " + dStores.toString() + "...");
    }
    System.out.println("Rebalancing...");
  }

  private synchronized static void mapRebalance(){
    for(String key : filesList.keySet()) {
      int tempSize = filesList.get(key).get(0);
      filesList.get(key).clear();
      filesList.get(key).add(tempSize);
    }
  }

  private synchronized static void updateHashMap (Integer port, ArrayList<String> files){
    System.out.println("Updating HashMap...");
    if (filesList.size() != 0 ){
      for (String file : files){
        if(filesList.containsKey(file)){
          filesList.get(file).add(port);
        }
      }
    }
    if (rebalanceCounter >= dStores.size()){
      System.out.println("HashMap updated, rebalancing...");
      rebalanceCounter = 0;

    }
  }

}

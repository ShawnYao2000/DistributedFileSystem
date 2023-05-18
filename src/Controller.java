
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Controller class for the Distributed File System
 * This class is responsible for the following:
 * 1. Accepting connections from Dstores
 * 2. Accepting connections from Clients
 * 3. Assigning Dstores to Clients
 * 4. Handling multiple requests from Clients
 */
class Controller {

  static Index index = new Index(new ArrayList<>(), new HashMap<>());
  static ArrayList<Integer> dStoreList = new ArrayList<>();
  static Map<Integer,Socket> dstorePortsSocket = new HashMap<>();
  static HashMap<String, ArrayList<Integer>> filesDStores = new HashMap<>();
  static HashMap<String, PrintWriter> storePrintWriter = new HashMap<>();
  static HashMap<String, PrintWriter> removeFilePW = new HashMap<>();

  static Logger controllerLogger = Logger.getLogger(Dstore.class.getName());

  static int rebalanceCounter = 0;
  static boolean rebalancing = false;
  static int totalRebalance = 0;
  static int rebalanceCompleteCounter = 0;
  static int storeComplete = 0;
  static int deleteComplete = 0;
  static int loadedPorts = 0;
  static int r = 0;
  static int cPort = 0;

  /**
   * Main method for the Controller
   * @param args
   */
  public static void main(String [] args){

    if(args.length<4){
      controllerLogger.info("Illegal Parameter for Controller");
      System.exit(0);
    }

    final int cport = Integer.parseInt(args[0]);
    final int r = Integer.parseInt(args[1]);
    final int timeout = Integer.parseInt(args[2]);
    final int rebalanceTime = Integer.parseInt(args[3]);
    setcPort(cport);
    setR(r);

    index.clear();
    updateHandlerIndex(index);
    dStoreList.clear();

    rebalanceTimer(rebalanceTime); //TODO: try to use thread.sleep instead?

    ClientHandler clientHandler = new ClientHandler(index);
    DStoreHandler dStoreHandler = new DStoreHandler(index);
    try{
      ServerSocket ss = new ServerSocket(cport);
      controllerLogger.info("Controller is listening on : [" + cport + "]");
      while(true){
        try{
          controllerLogger.info("Controller Waiting for connection...");
          Socket client = ss.accept();
          controllerLogger.info("Controller is Connected to a client at port : [" + cport + "]");
          new Thread(new Runnable() {
            @Override
            public void run() {
              AtomicInteger currentPort = new AtomicInteger();
              try {
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                AtomicBoolean fromDStore = new AtomicBoolean(false);
                String line;
                ExecutorService executor = Executors.newFixedThreadPool(Integer.MAX_VALUE); //TODO: test timeout
                while ((line = in.readLine()) != null) {
                  String[] contents = line.split(" ");
                  String command = contents[0];
                  controllerLogger.info(command + " <- [" + cport + "]");
                  String finalLine = line;
                  Callable<Void> callable = () -> {
                    switch (command) {
                      case Token.JOIN_TOKEN:
                        fromDStore.set(true);
                        currentPort.set(Integer.parseInt(contents[1]));
                        controllerLogger.info("Dstore ["+currentPort+"]" + " joined");
                        joinDS(contents[1]);
                        handleJoinRequest(client, finalLine);
                        rebalance();//TODO: test
                        break;

                      case "LIST":
                        if (fromDStore.get()) {
                          rebalanceCounter++;
                          controllerLogger.info("LIST" + " <- [" + cport + "]");
                          ArrayList<String> fileNames = new ArrayList<>();
                          for (int i = 1; i < contents.length; i++) {
                            fileNames.add(contents[i]);
                          }
                          updateIndex(currentPort.get(), fileNames);
                        } else {
                          if(sufficientDS(out)){
                            clientHandler.handleListRequest(out);
                            controllerLogger.info("LIST" + " <- [" + cport + "]");
                          } else {
                            controllerLogger.info(Token.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                          }
                        }
                        break;

                      case Token.STORE_TOKEN:
                        if(sufficientDS(out)) {
                          if(!index.fileStats.containsKey(contents[1])) {
                            storePrintWriter.put(contents[1], out);
                            controllerLogger.info("STORE" + " <- [" + cport + "]");
                          }
                          clientHandler.handleStoreRequest(out, r,  contents[1], Integer.parseInt(contents[2]));
                          controllerLogger.info("STORE" + " <- [" + cport + "]");
                          controllerLogger.info("File: " + contents[1] + " already exists in map.");//not sure if this will occur
                        } else {
                          controllerLogger.info(Token.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        }
                        break;

                      case Token.STORE_ACK_TOKEN:
                        if (index.fileStats.get(contents[1]) == Token.STORE_IN_PROGRESS) {
                          controllerLogger.info("STORE_ACK" + " <- [" + cport + "]");
                          dStoreHandler.handleStoreComplete(currentPort.get(), storePrintWriter.get(contents[1]), getCompletedStores() + 1, contents[1]);
                        }
                        break;

                      case Token.LOAD_TOKEN:
                        if (sufficientDS(out)) {
                          if(index.files.contains(contents[1])) {
                            setLoadedPorts(1);
                            clientHandler.handleLoadRequest(out, contents[1], getLoadedPorts());
                            controllerLogger.info("LOAD" + " <- [" + cport + "]");
                            controllerLogger.info("Loading file: [" + contents[1] +"]");
                          } else {
                            out.println(Token.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            controllerLogger.info(
                                Token.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " -> [" + cport + "]");
                          }
                        }
                        break;

                      case Token.RELOAD_TOKEN:
                        if (getLoadedPorts() == filesDStores.get(contents[1]).size()-1) {
                          out.println(Token.ERROR_LOAD_TOKEN);
                        } else if (sufficientDS(out)){
                          if(index.files.contains(contents[1])) {
                            setLoadedPorts(getLoadedPorts()+1);
                            clientHandler.handleLoadRequest(out, contents[1], getLoadedPorts());
                          } else {
                            out.println(Token.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            controllerLogger.info(
                                Token.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " -> [" + cport + "]");
                          }
                        }
                        break;

                      case Token.REMOVE_TOKEN:
                        if (sufficientDS(out) && index.files.contains(contents[1])){
                          System.out.println("IM REMOVING HERE!!!!");
                          removeFilePW.put(contents[1], out);
                          clientHandler.handleRemoveRequest(out, contents[1]);
                        } else {
                          out.println(Token.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                          ArrayList<Integer> dStores = Controller.filesDStores.get(contents[1]);
                          for (int i = 1; i < dStores.size(); i++) {
                            try {
                              Socket ds = new Socket(InetAddress.getLoopbackAddress(), dStores.get(i));
                              PrintWriter dStorePW = new PrintWriter(ds.getOutputStream(), true);
                              dStorePW.println(Token.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            } catch (Exception e) {
                              e.printStackTrace();
                              controllerLogger.info("Error sending message to DStore [" + dStores.get(i) + "]");
                            }
                          }
                        }
                        break;

                      case Token.REMOVE_ACK_TOKEN:
                        if (index.fileStats.get(contents[1]) == Token.REMOVE_IN_PROGRESS && sufficientDS(out)){
                          dStoreHandler.handleRemoveComplete(currentPort.get(), removeFilePW.get(contents[1]), getDeleteComplete() + 1, contents[1]);
                        } else {
                          System.out.println("!!!REMOVE FAILED!!!!");
                        }
                        break;

                      case Token.REBALANCE_COMPLETE_TOKEN:
                        out.println("LIST");
                        setRebalanceCompleteCounter(getRebalanceCompleteCounter()-1);
                        if(getRebalanceCompleteCounter() == 0){
                          postRebalanceRecover();
                        }
                        break;

                      default:
                        controllerLogger.info("INVALID MESSAGE: " + finalLine);
                        controllerLogger.info("USAGE: JOIN <port> | LIST | STORE <filename> <filesize> | LOAD <filename> | REMOVE <filename>");
                        break;
                    }
                    return null;
                  };
                  Future<Void> future = executor.submit(callable);
                  try {
                    future.get(timeout, TimeUnit.MILLISECONDS);
                  } catch (TimeoutException e) {
                    controllerLogger.info("Timeout occurred for command " + command);
                    e.printStackTrace();
                  } catch (Exception e) {
                    controllerLogger.info("Exception occurred for command " + command + ": " + e.getMessage());
                    e.printStackTrace();
                  }
                }
                executor.shutdown();
                client.close();

              } catch (SocketException e) {
                controllerLogger.info("DStore [" + currentPort + "] has failed");
                //recoverDS(currentPort.get());//TODO: CHECK
              } catch (IOException e) {
                e.printStackTrace();
                controllerLogger.info("Unexpected error occurred" + e);
              }
            }
          }).start();
        } catch (SocketTimeoutException e){
          controllerLogger.info("error "+e);
        } catch(Exception e){
          controllerLogger.info("error "+e);
        }
      }
    } catch(Exception e){controllerLogger.info("error "+e);}
  }

  /**
   * timer for rebalance
   * runs every:
   * @param time
   */
  private synchronized static void rebalanceTimer(int time){
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        try{
          rebalance();
        }
        catch (Exception e){
          controllerLogger.info("rebalance time out");
        }
      }
    }, time, time);
  }

  private synchronized static void rebalance() {

    controllerLogger.info("Starting rebalance operation.");

    // Calculate the number of files each DStore should store
    int F = index.files.size();
    int N = dStoreList.size();
    int lowerBound = (r * F) / N;
    int upperBound = (int) Math.ceil((double) (r * F) / N);

    // For each DStore, get the list of files
    invokeList();

    // TODO: Wait for LIST responses and update the index and filesDStores maps

    // For each DStore, calculate the files to send and remove
    for (Integer port : dStoreList) {
      ArrayList<String> filesToSend = new ArrayList<>();
      ArrayList<String> filesToRemove = new ArrayList<>();

      // Get the list of files currently stored in this DStore
      ArrayList<String> currentFiles = new ArrayList<>();
      for (Map.Entry<String, ArrayList<Integer>> entry : filesDStores.entrySet()) {
        if (entry.getValue().contains(port)) {
          currentFiles.add(entry.getKey());
        }
      }

      // If the DStore has more files than the upper bound, remove some files
      while (currentFiles.size() > upperBound) {
        String fileToRemove = currentFiles.remove(currentFiles.size() - 1);
        filesToRemove.add(fileToRemove);
      }

      // If the DStore has fewer files than the lower bound, add some files
      while (currentFiles.size() < lowerBound) {
        for (String file : index.files) {
          if (!currentFiles.contains(file)) {
            currentFiles.add(file);
            filesToSend.add(file);
            break;
          }
        }
      }

      // Send the REBALANCE command to the DStore
      try {
        Socket dStoreSocket = new Socket(InetAddress.getLoopbackAddress(), port);
        PrintWriter out = new PrintWriter(dStoreSocket.getOutputStream(), true);
        out.println(Token.REBALANCE_TOKEN + " " + filesToSend.size() + " " + String.join(" ", filesToSend) + " " + filesToRemove.size() + " " + String.join(" ", filesToRemove));
        controllerLogger.info("Rebalance message: [" +Token.REBALANCE_TOKEN + " " + filesToSend.size() + " " + String.join(" ", filesToSend) + " " + filesToRemove.size() + " " + String.join(" ", filesToRemove) + "] sent to DStore [" + port + "]");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // Reset the rebalancing flag
    controllerLogger.info("Rebalance operation complete.");
  }


  private synchronized static void handleJoinRequest(Socket socket, String command){
    String[] param = command.split(" ");
    try {
      int port = Integer.parseInt(param[1]);
        dstorePortsSocket.put(port,socket);
        controllerLogger.info("Putting port "+port+" in dstorePortsSocket");
      } catch (NumberFormatException e) {
      System.out.println("INVALID PARAMETER");
    }
  }


  /**
   * Check if there are enough DStores to handle the request
   * @param printWriter
   * @return
   */
  private synchronized static boolean sufficientDS(PrintWriter printWriter){
    if(dStoreList.size() < r){
      printWriter.println(Token.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return false;
    } else {
      return true;
    }
  }

  /**
   * Method to handle the JOIN command
   * @param port
   */
  private synchronized static void joinDS(String port){
    if (dStoreList.contains(Integer.valueOf(port))) {
      controllerLogger.info("DStore [" + port + "] already exists");
      return;
    } else {
      try {
        dStoreList.add(Integer.valueOf(port));
        controllerLogger.info("DStore [" + port + "] has been added");
      } catch (NumberFormatException e) {
        controllerLogger.info("Error adding DStore [" + port + "]");
      } catch (Exception e) {
        controllerLogger.info("Error adding DStore [" + port + "]");
      }
    }
  }

  /**
   * Sends a LIST request to all DStores
   */
  private synchronized static void invokeList(){
    for(int i = 0; i < dStoreList.size(); i++){
      try {
        Socket ds = new Socket(InetAddress.getLoopbackAddress(), dStoreList.get(i));
        PrintWriter dStorePW = new PrintWriter(ds.getOutputStream(), true);
        dStorePW.println("LIST");
      } catch (IOException e) {
        controllerLogger.info("Error sending LIST to DStore [" + dStoreList.get(i) + "]");
      }
      controllerLogger.info("LIST sent to DStore");
    }
  }

  /**
   * updates the index with all dstores
   */
  private synchronized static void updateIndex(Integer port, ArrayList<String> fileNames) {
    controllerLogger.info("Updating index at DStore [" + port + "]");
    if (!filesDStores.isEmpty()) {
      fileNames.forEach(f -> filesDStores.computeIfPresent(f, (key, value) -> {
        value.add(port);
        return value;
      }));
    }
    if (rebalanceCounter >= dStoreList.size()) {
      controllerLogger.info("Index updated!");
      controllerLogger.info("Rebalancing...");
    }
  }

  /**
   * recovers the files and updates index after rebalance
   */
  private synchronized static void postRebalanceRecover() {
    index.fileStats.entrySet().removeIf(entry -> {
      if (entry.getValue() == Token.REMOVE_IN_PROGRESS) {
        index.files.remove(entry.getKey());
        updateHandlerIndex(index);
        return true;
      }
      return false;
    });
  }


  public synchronized static void sendRemoveRequest(String fileName) {

    //PSEUDO
    ArrayList<Integer> dStores = Controller.filesDStores.get(fileName);
    for (int i = 1; i < dStores.size(); i++) {
      try {
        System.out.println("Sending REMOVE " + fileName + " to DStore [" + dStores.get(i) + "]");
        Socket ds = dstorePortsSocket.get(dStores.get(i));
        System.out.println("REMOVE PORT: " + ds.getPort() + ";Local Port" + ds.getLocalPort());
        PrintWriter dStorePW = new PrintWriter(ds.getOutputStream());
        dStorePW.println("REMOVE " + fileName);
        dStorePW.flush();
        controllerLogger.info("[" + ds.getLocalPort() + "]" + "->" + "[" + ds.getPort() + "]: REMOVE " + fileName);
      } catch (Exception e) {
        controllerLogger.info("Error removing file [" + fileName + "] from DStore [" + dStores.get(i) + "]");
        e.printStackTrace();
      }
    }
    //PSEUDO

    for (int i = 1; i < dStores.size(); i++) {
      try {
        Socket ds = new Socket(InetAddress.getLoopbackAddress(), dStores.get(i));
        PrintWriter dStorePW = new PrintWriter(ds.getOutputStream(), true);
        dStorePW.println("REMOVE " + fileName);
        controllerLogger.info("[" + ds.getLocalPort() + "]" + "->" + "[" + ds.getPort() + "]: REMOVE " + fileName);
      } catch (Exception e) {
        controllerLogger.info("Error removing file [" + fileName + "] from DStore [" + dStores.get(i) + "]");
      }
    }
  }

  /**
   * updates the index in the handlers
   * @param index
   */
  private synchronized static void updateHandlerIndex(Index index){
    DStoreHandler.updateIndex(index);
    ClientHandler.updateIndex(index);
  }


  //GETTERS AND SETTERS

  public static int getR() {
    return r;
  }

  public static void setR(int r) {
    Controller.r = r;
  }

  public static void setcPort(int cPort) {
    Controller.cPort = cPort;
  }

  public static int getcPort() {
    return cPort;
  }

  public static int getCompletedStores() {
    return storeComplete;
  }

  public static void setCompletedStores(int completed_stores) {
    Controller.storeComplete = completed_stores;
  }

  public static int getLoadedPorts() {
    return loadedPorts;
  }

  public static void setLoadedPorts(int loadedPorts) {
    Controller.loadedPorts = loadedPorts;
  }

  public static int getDeleteComplete() {
    return deleteComplete;
  }

  public static void setDeleteComplete(int deleteComplete) {
    Controller.deleteComplete = deleteComplete;
  }

  public static void deleteRecycle(){
    Controller.deleteComplete = 0;
  }

  public static void cycleRebalance(){
    Controller.rebalanceCounter = 0;
  }

  public static void storeRecycle(){
    Controller.storeComplete = 0;
  }

  public static int getRebalanceCompleteCounter() {
    return rebalanceCompleteCounter;
  }

  public static void setRebalanceCompleteCounter(int rebalanceCompleteCounter) {
    Controller.rebalanceCompleteCounter = rebalanceCompleteCounter;
  }
}

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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  static HashMap<String, ArrayList<Integer>> filesList = new HashMap<>();

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
    setR(r);

    index.clear();
    updateHandlerIndex(index);
    dStoreList.clear();

    rebalanceTimer(rebalanceTime); //TODO: try to use thread.sleep instead

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
                      case Protocol.JOIN_TOKEN:
                        fromDStore.set(true);
                        currentPort.set(Integer.parseInt(contents[1]));
                        controllerLogger.info("Dstore ["+currentPort+"]" + " joined");
                        joinDS(contents[1]);
                        rebalance();
                        break;

                        //DS STARTS WITH JOIN?
                        //TODO: CHECK SPEC
                      case Protocol.LIST_TOKEN:
                        if (!fromDStore.get()) {
                          if (sufficientDS(out)){
                            clientHandler.handleListRequest(out);
                            controllerLogger.info("LIST" + " <- [" + cport + "]");
                          }
                        } else {
                          rebalanceCounter++;
                          controllerLogger.info("LIST" + " <- [" + cport + "]");
                          ArrayList<String> fileNames = new ArrayList<>();
                          for(int i = 1; i < contents.length; i++){
                            fileNames.add(contents[i]);
                          }
                          updateIndex(currentPort.get(), fileNames);
                        }
                        break;

                      case Protocol.STORE_TOKEN:
                        if(sufficientDS(out)) {
                          if(!index.fileStats.containsKey(contents[1])) {
                            storePrintWriter.put(contents[1], out);
                            controllerLogger.info("STORE" + " <- [" + cport + "]");
                          }
                          clientHandler.handleStoreRequest(out, r,  contents[1], Integer.parseInt(contents[2]));
                          controllerLogger.info("STORE" + " <- [" + cport + "]");
                          controllerLogger.info("File: " + contents[1] + " already exists in map.");//not sure if this will occur
                        } else {
                          controllerLogger.info("ERROR_NOT_ENOUGH_DSTORES");
                        }
                        break;

                      case Protocol.STORE_ACK_TOKEN:
                        if (index.fileStats.get(contents[1]) == Protocol.STORE_IN_PROGRESS) {
                          controllerLogger.info("STORE_ACK" + " <- [" + cport + "]");
                          dStoreHandler.handleStoreComplete(currentPort.get(), storePrintWriter.get(contents[1]), getCompletedStores() + 1, contents[1]);
                        }
                        break;

                      case Protocol.LOAD_TOKEN:
                        if (sufficientDS(out)) {
                          if(index.files.contains(contents[1])) {
                            setLoadedPorts(1);
                            clientHandler.handleLoadRequest(out, contents[1], getLoadedPorts());
                            controllerLogger.info("LOAD" + " <- [" + cport + "]");
                            controllerLogger.info("Loading file: [" + contents[1] +"]");
                          } else {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                            controllerLogger.info("ERROR_FILE_DOES_NOT_EXIST" + " -> [" + cport + "]");
                          }
                        }
                        break;

                      case Protocol.RELOAD_TOKEN:
                        if (getLoadedPorts() == filesList.get(contents[1]).size()-1) {
                          out.println(Protocol.ERROR_LOAD_TOKEN);
                        } else if (sufficientDS(out)){
                          if(index.files.contains(contents[1])) {
                            setLoadedPorts(getLoadedPorts()+1);
                            clientHandler.handleLoadRequest(out, contents[1], getLoadedPorts());
                          } else {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                            controllerLogger.info("ERROR_FILE_DOES_NOT_EXIST" + " -> [" + cport + "]");
                          }
                        }
                        break;

                      case Protocol.REMOVE_TOKEN:
                        if (sufficientDS(out) && index.files.contains(contents[1])){
                          removeFilePW.put(contents[1], out);
                          clientHandler.handleRemoveRequest(out, contents[1]);
                        } else {
                          out.println("ERROR_FILE_DOES_NOT_EXIST");
                          controllerLogger.info("ERROR_FILE_DOES_NOT_EXIST" + " -> [" + cport + "]");
                        }
                        break;

                      case Protocol.REMOVE_ACK_TOKEN:
                        if (index.fileStats.get(contents[1]) == Protocol.REMOVE_IN_PROGRESS && sufficientDS(out)){
                          dStoreHandler.handleRemoveComplete(currentPort.get(), removeFilePW.get(contents[1]), getDeleteComplete() + 1, contents[1]);
                        } else {
                          System.out.println("!!!REMOVE FAILED!!!!");
                        }
                        break;

                      case Protocol.REBALANCE_COMPLETE_TOKEN:
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
                recoverDS(currentPort.get());
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
   * Check if there are enough DStores to handle the request
   * @param printWriter
   * @return
   */
  private synchronized static boolean sufficientDS(PrintWriter printWriter){
    if(dStoreList.size() < r){
      printWriter.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
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

  /**
   * main rebalance method, calls invokelist() and set rebalancing to true
   */
  private synchronized static void rebalance(){
    recoverIndex();
    if(!dStoreList.isEmpty()){
      rebalancing = true;
      controllerLogger.info("Rebalance invoked...");
    } else {
      controllerLogger.info("No DStores to rebalance");
    }
    invokeList();// invoke anyway?
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
    if (filesList.size() != 0) {
      for (String f : fileNames) {
        if(filesList.containsKey(f)) {
          filesList.get(f).add(port);
        }
      }
    }
    if (rebalanceCounter >= dStoreList.size()) {
      controllerLogger.info("Index updated!");
      controllerLogger.info("Rebalancing...");
      cycleRebalance();
      calcRebalance();
    }
  }

  /**
   * fill in the filesList
   */
  private synchronized static void recoverIndex(){
    for(String key : filesList.keySet()) {
      int size = filesList.get(key).get(0);
      filesList.get(key).clear();
      filesList.get(key).add(size);
    }
  }

  /**
   * rebalance calculation
   * huge ass........ TODO: TRY TO SHORTEN THIS
   */
  private synchronized static void calcRebalance() {
    String minFile;
    Integer minDStore;
    String maxFile;
    Integer maxDStore;

    HashMap<Integer, ArrayList<String>> dStoreHM = getDStores();

    Double minRep = Math.floor(Double.valueOf(r * filesList.size()) / Double.valueOf(dStoreList.size()));
    Double maxRep = Math.ceil(Double.valueOf(r * filesList.size()) / Double.valueOf(dStoreList.size()));

    HashMap<String, ArrayList<Integer>> toSend = new HashMap<>();
    HashMap<Integer, ArrayList<String>> toStore = new HashMap<>();
    HashMap<Integer, ArrayList<String>> toDelete = deleteFailedFiles(index.fileStats);

    while (!isFinished(dStoreHM, r, minRep, maxRep)) {
      int index = 0;
      minDStore = getMinDStore(dStoreHM);
      maxDStore = getMaxDStore(dStoreHM);
      minFile = getMinFile(index, dStoreHM);
      maxFile = getMaxFile(dStoreHM, maxDStore);

      while (!isOptimal(minDStore, maxDStore, minFile, maxFile, minRep, maxRep, dStoreHM)) {
        if (dStoreHM.get(minDStore).contains(minFile)) {
          index++;
          minFile = getMinFile(index, dStoreHM);
        } else if (dStoreHM.get(maxDStore).contains(maxFile)) {
          maxFile = getMaxFile(dStoreHM, maxDStore);
        } else {
          break;
        }
      }

      if (!isOptimal(minDStore, maxDStore, minFile, maxFile, minRep, maxRep, dStoreHM)) {
        continue;
      }

      dStoreHM.get(minDStore).add(minFile);
      dStoreHM.get(maxDStore).remove(maxFile);

      toStore.computeIfAbsent(minDStore, k -> new ArrayList<>()).add(minFile);
      toDelete.computeIfAbsent(maxDStore, k -> new ArrayList<>()).add(maxFile);

      ArrayList<Integer> sendList = toSend.computeIfAbsent(minFile, k -> new ArrayList<>());
      if (!sendList.contains(minDStore)) {
        sendList.add(minDStore);
      }
    }

    preRebalance(toStore, toDelete, toSend);
  }

  /**
   * just a helper to check if opti
   * @param minDStore
   * @param maxDStore
   * @param minFile
   * @param maxFile
   * @param minRep
   * @param maxRep
   * @param dStoreHM
   * @return if optimal
   */
  private static boolean isOptimal(Integer minDStore, Integer maxDStore, String minFile, String maxFile,
      Double minRep, Double maxRep, HashMap<Integer, ArrayList<String>> dStoreHM) {
    return dStoreHM.get(minDStore).size() < minRep && !dStoreHM.get(minDStore).contains(minFile)
        && dStoreHM.get(maxDStore).size() > maxRep && dStoreHM.get(maxDStore).contains(maxFile);
  }

  /**
   * remove the failed files, returns for sani
   * @param statusHM
   * @return
   */
  private synchronized static HashMap<Integer, ArrayList<String>> deleteFailedFiles(HashMap<String, Integer> statusHM) {
    HashMap<Integer, ArrayList<String>> filesToRemove = new HashMap<>();
    for (Map.Entry<String, Integer> entry : statusHM.entrySet()) {
      String key = entry.getKey();
      Integer status = entry.getValue();
      if (status == Protocol.REMOVE_IN_PROGRESS) {
        ArrayList<Integer> ports = filesList.get(key);
        ports.remove(0);
        for (Integer port : ports) {
          filesToRemove.computeIfAbsent(port, k -> new ArrayList<>()).add(key);
        }
      }
    }
    return filesToRemove;
  }


  /**
   * get dstores hash map
   * @return
   */
  private synchronized static HashMap<Integer, ArrayList<String>> getDStores() {
    index.files.removeIf(key -> index.fileStats.get(key) == Protocol.REMOVE_IN_PROGRESS);
    filesList.keySet().removeAll(index.files);

    HashMap<Integer, ArrayList<String>> dStoreHM = new HashMap<>();
    for (String key : filesList.keySet()) {
      ArrayList<Integer> ports = filesList.get(key);
      for (int i = 1; i < ports.size(); i++) {
        Integer port = ports.get(i);
        dStoreHM.computeIfAbsent(port, k -> new ArrayList<>()).add(key);
      }
    }

    for (Integer port : dStoreList) {
      dStoreHM.putIfAbsent(port, new ArrayList<>());
    }
    return dStoreHM;
  }

  /**
   * get the maximum ds
   * @param dStoreHM
   * @return
   */
  private static Integer getMinDStore(HashMap<Integer, ArrayList<String>> dStoreHM) {
    return dStoreHM.entrySet().stream()
        .min(Comparator.comparingInt(entry -> entry.getValue().size()))
        .map(Map.Entry::getKey)
        .orElse(1);
  }

  /**
   * get the maximum dstores
   * @param dStoreHM
   * @return
   */
  private static Integer getMaxDStore(HashMap<Integer, ArrayList<String>> dStoreHM) {
    return dStoreHM.entrySet().stream()
        .max(Comparator.comparingInt(entry -> entry.getValue().size()))
        .map(Map.Entry::getKey)
        .orElse(1);
  }

  /**
   * get the minimum file in the dStore
   * @param skip
   * @param hm
   * @return
   */
  private static String getMinFile(Integer skip, HashMap<Integer, ArrayList<String>> hm) {
    HashSet<String> filesSet = hm.values().stream().flatMap(Collection::stream).collect(Collectors.toCollection(HashSet::new));
    HashMap<String, ArrayList<Integer>> invertedHM = fileToKeys(hm);
    return IntStream.range(0, skip + 1)
        .mapToObj(i -> {
          String minFile = getMinFileAbs(filesSet, invertedHM);
          filesSet.remove(minFile);
          return minFile;
        })
        .skip(skip)
        .findFirst()
        .orElse("");
  }

  /**
   * get the maximum file in the dStore
   * @param hm
   * @param maxDStore
   * @return
   */
  private static String getMaxFile(HashMap<Integer, ArrayList<String>> hm, Integer maxDStore) {
    ArrayList<String> fileArray = hm.get(maxDStore);
    HashMap<String, ArrayList<Integer>> invertedHM = fileToKeys(hm);
    return fileArray.stream()
        .max(Comparator.comparingInt(key -> invertedHM.get(key).size()))
        .orElse("");
  }

  /**
   * get the minimum file in the dStore
   * @param stringSet
   * @param hm
   * @return
   */
  private static String getMinFileAbs(Set<String> stringSet, HashMap<String, ArrayList<Integer>> hm) {
    return stringSet.stream()
        .min(Comparator.comparingInt(key -> hm.get(key).size()))
        .orElse("");
  }

  /**
   * convert the hashmap from key to file to file to key
   * @param hm
   * @return
   */
  private static HashMap<String, ArrayList<Integer>> fileToKeys(HashMap<Integer, ArrayList<String>> hm) {
    HashMap<String, ArrayList<Integer>> converted = new HashMap<>();
    hm.forEach((key, fileList) -> fileList.forEach(file -> converted.computeIfAbsent(file, k -> new ArrayList<>()).add(key)));
    return converted;
  }

  /**
   * check if the rebalance is finished
   * @param dStoreHM
   * @param rep_factor
   * @param minRep
   * @param maxRep
   * @return
   */
  private static boolean isFinished(HashMap<Integer, ArrayList<String>> dStoreHM, Integer rep_factor, Double minRep, Double maxRep) {
    HashMap<String, ArrayList<Integer>> fileHM = fileToKeys(dStoreHM);
    return fileHM.values().stream().allMatch(list -> list.size() == rep_factor)
        && dStoreHM.values().stream().allMatch(list -> list.size() >= minRep && list.size() <= maxRep);
  }

  /**
   * This method is called when a rebalance is needed. It will send the files to the appropriate DStores.
   * @param fileToStoreHM
   * @param fileToRemoveHM
   * @param fileToSendHM
   */
  private synchronized static void preRebalance(HashMap<Integer, ArrayList<String>> fileToStoreHM, HashMap<Integer, ArrayList<String>> fileToRemoveHM, HashMap<String, ArrayList<Integer>> fileToSendHM) {
    if (!fileToStoreHM.isEmpty() || !fileToRemoveHM.isEmpty() || !fileToSendHM.isEmpty()) {
      Map<Map.Entry<Integer, String>, List<Integer>> finalSend = fileToSendHM.entrySet().stream().collect(Collectors.toMap(
          entry -> new AbstractMap.SimpleEntry<>(entry.getValue().get(0), entry.getKey()),
          entry -> entry.getValue().subList(1, entry.getValue().size())));

      setRebalanceCompleteCounter(0);
      dStoreList.forEach(port -> {
        String filesToSend = finalSend.entrySet().stream()
            .filter(entry -> entry.getKey().getKey().equals(port))
            .map(entry -> " " + entry.getKey().getValue() + " " + entry.getValue().size() + portsToMessage((ArrayList<Integer>) entry.getValue()))
            .collect(Collectors.joining());
        filesToSend = finalSend.entrySet().stream().filter(entry -> entry.getKey().getKey().equals(port)).count() + filesToSend;
        String filesToRemove = fileToRemoveHM.getOrDefault(port, new ArrayList<>()).stream()
            .collect(Collectors.joining(" ", Integer.toString(fileToRemoveHM.getOrDefault(port, new ArrayList<>()).size()), ""));
        rebalanceRequest(port, filesToSend, filesToRemove);
      });
    } else {
      controllerLogger.info("No rebalance needed!");
    }
  }

  /**
   * recover a given ds
   * @param port
   */
  private synchronized static void recoverDS(Integer port) {
    if (port == 0) return;
    dStoreList.remove(port);
    filesList.entrySet().removeIf(entry -> {
      entry.getValue().remove(port);
      if (entry.getValue().size() == 1) {
        index.files.remove(entry.getKey());
        index.fileStats.remove(entry.getKey());
        updateHandlerIndex(index);
        return true;
      }
      return false;
    });
  }

  /**
   * recovers the files and updates index after rebalance
   */
  private synchronized static void postRebalanceRecover() {
    index.fileStats.entrySet().removeIf(entry -> {
      if (entry.getValue() == Protocol.REMOVE_IN_PROGRESS) {
        index.files.remove(entry.getKey());
        updateHandlerIndex(index);
        return true;
      }
      return false;
    });
  }


  /**
   * converts arraylist of files to string
   * @param filesList
   * @return
   */
  private static String filesToMessage(ArrayList<String> filesList) {
    String output = "";
    for (String file : filesList) {
      output += " " + file;
    }
    return output;
  }

  /**
   * converts arraylist of ports to string
   * @param portsList
   * @return
   */
  private static String portsToMessage(ArrayList<Integer> portsList) {
    String output = "";
    for (Integer port : portsList) {
      output += " " + port.toString();
    }
    return output;
  }

  /**
   * sends rebalance request to dstore
   * @param port
   * @param files_to_send
   * @param files_to_remove
   */
  private static void rebalanceRequest(Integer port, String files_to_send, String files_to_remove){
    controllerLogger.info("REBALANCE " + files_to_send + " " + files_to_remove);
    try {
      Socket dStore = new Socket(InetAddress.getLoopbackAddress(), port);
      PrintWriter dStorePW = new PrintWriter(dStore.getOutputStream(), true);
      dStorePW.println("REBALANCE " + files_to_send + " " + files_to_remove);
      setRebalanceCompleteCounter(getRebalanceCompleteCounter()+1);
    } catch (IOException e) {
      e.printStackTrace();
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
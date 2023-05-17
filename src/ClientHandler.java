import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.logging.Logger;

class ClientHandler {
  private static Index index;
  static Logger handlerLogger = Logger.getLogger(ClientHandler.class.getName());
  private static int lastUsedDStoreIndex = 0;

  /**
   * This class handles message from clients
   * @param index
   */
  public ClientHandler(Index index) {
    this.index = index;
  }

  public synchronized static void updateIndex(Index index) {
    ClientHandler.index = index;
  }

  /**
   * handles list request from client
   * @param printWriter
   */
  public synchronized static void handleListRequest(PrintWriter printWriter){
    String file_names = "";
    if(index.getFiles().size() > 0) {
      for(String file : index.getFiles()){
        file_names = file_names + " " + file;
      }
    }
    printWriter.println("LIST" + file_names);
    handlerLogger.info("List sent to controller");
  }

  /**
   * handles store request from client
   * @param printWriter
   * @param r
   * @param fileName
   * @param param
   */
  public synchronized static void handleStoreRequest(PrintWriter printWriter, int r, String fileName, int param) {
    if(index.fileStats.containsKey(fileName) && index.files.contains(fileName)){
      printWriter.println(Token.ERROR_FILE_ALREADY_EXISTS_TOKEN);
      handlerLogger.info("File [" + fileName + "] already exists");
    } else {
      ArrayList<Integer> id = new ArrayList<>();
      id.add(param);
      Controller.filesDStores.put(fileName, id);
      System.out.println(Controller.filesDStores);
      index.fileStats.put(fileName, Token.STORE_IN_PROGRESS); //index updated to "store in progress"
      handlerLogger.info("Index updated to \"Store in progress\"!");
      String ports = "";

      // Loop to select r DStores
      for (int i = 0; i < r; i++) {
        // Use modulo operation to ensure the index stays within the bounds of the DStore list
        int dStoreIndex = (lastUsedDStoreIndex + i) % Controller.dStoreList.size();
        ports += " " + Controller.dStoreList.get(dStoreIndex);
      }

      // Update last used DStore index
      lastUsedDStoreIndex = (lastUsedDStoreIndex + r) % Controller.dStoreList.size();

      printWriter.println(Token.STORE_TO_TOKEN + ports);
      handlerLogger.info("Store request sent to DStores at ports[" + ports +"]");
    }
  }


  /**
   * handles remove request from client
   * @param clientPrintWriter
   * @param fileName
   */
  public synchronized static void handleRemoveRequest(PrintWriter clientPrintWriter, String fileName){
    System.out.println("Removing file [" + fileName + "]");
    if(index.files.contains(fileName)) {
      index.fileStats.remove(fileName);
      index.files.remove(fileName);
      index.fileStats.put(fileName, Token.REMOVE_IN_PROGRESS); //remove in progress
      handlerLogger.info("Index updated to \"Remove in progress\"");
      if (Controller.filesDStores.containsKey(fileName)) {
        Controller.sendRemoveRequest(fileName);
      }
    } else {
      clientPrintWriter.println("ERROR_FILE_DOES_NOT_EXIST");
      handlerLogger.info("File [" + fileName + "] does not exist");
    }
  }


  /**
   * handles load request from client
   * @param printWriter
   * @param fileName
   * @param loadPortCounter
   */
  public synchronized static void handleLoadRequest(PrintWriter printWriter, String fileName, int loadPortCounter){
    for(String file : Controller.filesDStores.keySet()){
      if(file.equals(fileName) && index.files.contains(file)){
        int fileSize = Controller.filesDStores.get(fileName).get(0);
        int port = Controller.filesDStores.get(fileName).get(loadPortCounter);
        handlerLogger.info("Loading file [" + fileName + "] from DStore [" + port + "]");
        printWriter.println("LOAD_FROM " + port + " " + fileSize);
      } else {
        handlerLogger.info("File [" + fileName + "] does not exist in DStores");
      }
    }
  }
}

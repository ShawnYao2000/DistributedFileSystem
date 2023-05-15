import java.io.PrintWriter;
import java.util.logging.Logger;

class DStoreHandler {
  private static Index index;
  static Logger DSHandlerLogger = Logger.getLogger(ClientHandler.class.getName());

  /**
   * This class handles message from dstores
   * @param index
   */
  public DStoreHandler(Index index) {
    this.index = index;
  }

  /**
   * update local index
   * @param index
   */
  public synchronized static void updateIndex(Index index) {
    DStoreHandler.index = index;
  }

  /**
   * handles store coplete from dstores
   * @param port
   * @param printWriter
   * @param completed_stores
   * @param fileName
   */
  public synchronized static void handleStoreComplete(int port, PrintWriter printWriter, int completed_stores, String fileName) {
    if(completed_stores == Controller.r) {
      index.files.add(fileName);
      DSHandlerLogger.info("Index updated with new file [" + fileName + "]");
      Controller.setCompletedStores(0);
      if(Controller.filesDStores.get(fileName).contains(port)) {
        DSHandlerLogger.info("File [" + fileName + "] already exists in DStore [" + port + "]");
      } else {
        Controller.filesDStores.get(fileName).add(port);
        Controller.filesDStores.put(fileName, Controller.filesDStores.get(fileName));
      }
      index.fileStats.remove(fileName);
      index.fileStats.put(fileName, Protocol.STORE_COMPLETE); //index updated to "store complete"
      DSHandlerLogger.info("Index updated to \"Store complete\"!");
      printWriter.println(Protocol.STORE_COMPLETE_TOKEN);
      DSHandlerLogger.info("File [" + fileName + "] has been stored in DStores");
    } else {
      Controller.setCompletedStores(Controller.getCompletedStores()+1);
      if(Controller.filesDStores.get(fileName).contains(port)) {
        DSHandlerLogger.info("File [" + fileName + "] already exists in DStore [" + port + "]");
      } else {
        Controller.filesDStores.get(fileName).add(port);
        Controller.filesDStores.put(fileName, Controller.filesDStores.get(fileName));
      }
    }
  }

  /**
   * Handles the remove complete message from dstores
   * @param port
   * @param printWriter
   * @param completed_deletes
   * @param fileName
   */
  public synchronized static void handleRemoveComplete(int port, PrintWriter printWriter, int completed_deletes, String fileName) {
    if (completed_deletes == Controller.filesDStores.get(fileName).size() - 1) {
      index.files.remove(fileName);
      Controller.setDeleteComplete(0);

      Controller.filesDStores.remove(fileName);

      index.fileStats.remove(fileName);
      index.fileStats.put(fileName, Protocol.REMOVE_COMPLETE); //index updated to "remove complete"
      DSHandlerLogger.info("Index updated to \"Remove complete\"!");
      printWriter.println("REMOVE_COMPLETE");
      DSHandlerLogger.info("Remove completed!");
    } else {
      Controller.setDeleteComplete(Controller.getDeleteComplete() + 1);

      Controller.filesDStores.remove(port);
    }
  }
}
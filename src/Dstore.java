import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Dstore {

  static String currentFileName;
  static int currentFileSize;
  static int cport;
  static int currentPort;
  static String currentFolder;
  static String fileToSend;
  static Logger dStoreLogger = Logger.getLogger(Dstore.class.getName());

  /**
   * Main method for Dstore
   * @param args
   */
  public static void main(String[] args) {

    final int port = Integer.parseInt(args[0]);
    final int cport = Integer.parseInt(args[1]);
    final int timeout = Integer.parseInt(args[2]);
    currentFolder = args[3];
    setPort(port);
    setCport(cport);
    setCurrentFolder(currentFolder);
    sanitize(currentFolder);

    try {
      ServerSocket listener = new ServerSocket(port);
      dStoreLogger.info("DStore is listening on port " + port);
      try {
        Socket controller = new Socket(InetAddress.getLoopbackAddress(), cport);
        PrintWriter controllerPrintWriter = new PrintWriter(controller.getOutputStream(), true);
        controllerPrintWriter.println("JOIN " + port);
        while(true) {
          dStoreLogger.info("DStore has joined at port[" + port + "], cport[" + cport + "]");
          dStoreLogger.info("Waiting for connection...");
          Socket client = listener.accept();
          dStoreLogger.info("Connection established at port[" + port + "], cport[" + cport + "]");
          new Thread(new Runnable() {
            @Override
            public void run() {
              try {
                OutputStream clientWriter = client.getOutputStream();
                PrintWriter printWriter = new PrintWriter(clientWriter, true);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                  String[] commands = line.split(" ");
                  switch (commands[0]) {

                    case Protocol.STORE_TOKEN:
                      dStoreLogger.info("[" + port + "] <- " + "[" + cport + "] : STORE " + commands[1] + " | size:" + commands[2]);
                      storeReq(printWriter, commands[1], Integer.parseInt(commands[2]));
                      client.setSoTimeout(timeout);
                      receiveFile(controllerPrintWriter, client.getInputStream(), currentFileSize, true);
                      break;

                    case Protocol.REBALANCE_STORE_TOKEN:
                      dStoreLogger.info("[" + port + "] <- " + "[" + cport + "] : REBALANCE_STORE " + commands[1] + " | size:" + commands[2]);
                      storeReq(printWriter, commands[1], Integer.parseInt(commands[2]));
                      receiveFile(printWriter, client.getInputStream(), currentFileSize, false);
                      break;

                    case Protocol.REMOVE_TOKEN:
                      dStoreLogger.info("[" + port + "] <- " + "[" + cport + "] : REMOVE " + commands[1]);
                      removeFile(controllerPrintWriter, commands[1]);
                      break;

                    case Protocol.LOAD_DATA_TOKEN:
                      dStoreLogger.info("[" + port + "] <- " + "[" + cport + "] : LOAD_DATA " + commands[1]);
                      loadFile(commands[1], clientWriter);
                      break;

                    case Protocol.LIST_TOKEN:
                      dStoreLogger.info("[" + port + "] <- " + "[" + cport + "] : LIST");
                      listFile(controllerPrintWriter);
                      break;
                    case Protocol.REBALANCE_TOKEN:
                      dStoreLogger.info("[" + port + "] <- " + "[" + cport + "] : REBALANCE " + commands[1]);
                      String remainingRebalance = line.split(" ", 2)[1];
                      rebalance(controllerPrintWriter, remainingRebalance);
                      break;
                    case Protocol.ACK_TOKEN:
                      dStoreLogger.info("[" + port + "] <- " + "[" + cport + "] : ACK");
                      writeFileContent(clientWriter);
                      break;
                    default:
                      dStoreLogger.info("Invalid command: [" + line + "] received");
                      break;
                  }
                }
                client.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }).start();
        }
      } catch(Exception e) { System.err.println("error: " + e); }
    } catch(Exception e) { System.err.println("error: " + e);}
  }

  /**
   * Method to handle rebalance
   * @param file_folder
   */
  private static void sanitize(String file_folder) {
    File directory = new File(file_folder);
    if(!directory.exists()){
      directory.mkdir();
    } else if(!isEmpty(file_folder)){
      for(File file : directory.listFiles()){
        try{
          file.delete();
        } catch(Exception e){
          System.out.println("Error deleting file: " + file.getName());
        }
      }
      dStoreLogger.info("Ready to receive files");
    }
  }

  /**
   * checks if directory is empty
   * @param path
   * @return
   */
  private static boolean isEmpty(String path) {
    try (Stream<Path> entries = Files.list(Path.of(path))) {
      return !entries.findFirst().isPresent();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Method to set the file names and size, sends out "ACK"
   * @param printWriter
   * @param fileName
   * @param fileSize
   */
  private synchronized static void storeReq(PrintWriter printWriter, String fileName, int fileSize) {
    try {
      currentFileName = fileName;
      currentFileSize = fileSize;
      printWriter.println("ACK");
      dStoreLogger.info("ACK -> " + "[" + cport + "]");
    } catch (Exception e) {
      dStoreLogger.info("Error sending ACK");
    }
  }

  /**
   * Method to receive file content
   * writes file to file folder
   * @param printWriter
   * @param inputStream
   * @param fileSize
   * @param storeAcked
   */
  private synchronized static void receiveFile(PrintWriter printWriter, InputStream inputStream, int fileSize, boolean storeAcked) {
    try {
      File file = new File(currentFolder + File.separator + currentFileName);
      if (fileSize!=file.length()){
        dStoreLogger.info("FILE SIZE DOESN'T MATCH");
        dStoreLogger.info("FILE SIZE DOESN'T MATCH, EXPECTED " + fileSize + " | ACTUAL" + file.length());
      }
      byte[] data = new byte[fileSize];
      inputStream.readNBytes(data, 0, fileSize);
      FileOutputStream outputStream = new FileOutputStream(file);
      outputStream.write(data);
    } catch (FileNotFoundException e) {
      dStoreLogger.info("File not found in absolute path: " + currentFolder + File.separator + currentFileName);
    } catch (IOException e) {
      dStoreLogger.info("Error writing file: " + currentFolder + File.separator + currentFileName);
    }
    if(storeAcked) {
      printWriter.println("STORE_ACK " + currentFileName);
    }
    dStoreLogger.info("File stored");
    dStoreLogger.info("STORE_ACK -> " + "[" + cport + "]");
  }

  /**
   * Method to load file content
   * @param fileName
   * @param out
   */
  private synchronized static void loadFile(String fileName, OutputStream out){
    try {
      dStoreLogger.info("Loading file " + fileName);
      File file = new File(currentFolder + File.separator + fileName);
      byte[] data = Files.readAllBytes(file.toPath());
      out.write(data);
      dStoreLogger.info("File loaded");
    } catch (IOException e) {
      dStoreLogger.info("Error loading file: " + currentFolder + File.separator + fileName);
      e.printStackTrace();
    }
  }


  /**
   * Method to remove file from file folder
   * Sends out "REMOVE_ACK" if file is removed
   * @param printWriter
   * @param fileName
   */
  private synchronized static void removeFile(PrintWriter printWriter, String fileName){
    Boolean deleted = false;
    File file = new File(currentFolder + File.separator + fileName);
    if(file.exists()) {
      try {
        file.delete();
        deleted = true;
      } catch (Exception e) {
        dStoreLogger.info("Error deleting file: " + currentFolder + File.separator + fileName);
      }
      if(deleted) {
        printWriter.println("REMOVE_ACK " + fileName);
        dStoreLogger.info("Removed");
      } else {
        dStoreLogger.info("Not Removed!");
      }
    } else {
      printWriter.println("ERROR_FILE_DOES_NOT_EXIST " + fileName);
      dStoreLogger.info("ERRROR_FILE_DOES_NOT_EXIST -> " + "[" + cport + "]");
    }
  }

  /**
   * Handles list request, list all the files in folder
   * Sends out "LIST" with all the file names
   * @param printWriter
   */
  private synchronized static void listFile(PrintWriter printWriter){
    String files = "";
    File folder = new File(currentFolder);
    String[] fileList = folder.list();
    for(String file : fileList){
      files = files + " " + file;
    }
    printWriter.println("LIST" + files);
    dStoreLogger.info("LIST -> " + "[" + cport + "]");
  }

  /**
   * Rebalance method
   * @param toController
   * @param commands
   */
  private synchronized static void rebalance(PrintWriter toController, String commands){
    String[] contents = commands.split(" ");
    int moved = 0;
    int counter = 1;
    int numFiles = Integer.valueOf(contents[0]);
    while(moved < numFiles){
      String fileName = contents[counter];
      int portsNum = Integer.valueOf(contents[counter+1]);
      for(int i = 1; i <= portsNum; i++){
        int port = Integer.valueOf(contents[counter+1+i]);
        writeFile(port, fileName);
      }
      moved++;
      counter = counter + portsNum + 2;
    }
    int reCounter = counter;
    int removed = Integer.valueOf(contents[reCounter]);
    for(int i = 1; i <= removed; i++){
      remove(contents[reCounter+i]);
    }
    toController.println("REBALANCE_COMPLETE");
    dStoreLogger.info("REBALANCE_COMPLETE -> " + "[" + cport + "]");
  }

  /**
   * handles rebalance when file is removed
   * @param fileName
   */
  private synchronized static void remove(String fileName){
    File file = new File(currentFolder + File.separator + fileName);
    if(file.exists()) {
      if(file.delete()) {
        dStoreLogger.info("[" + fileName + "] removed");
      } else {
        dStoreLogger.info("Error removing file"+ fileName);
      }
    }
  }


  /**
   * Method to send file to other servers
   * @param port
   * @param fileName
   */
  private synchronized static void writeFile(Integer port, String fileName){
    try {
      long fileSize = Files.size(Path.of(currentFolder + File.separator + fileName));
      Socket dStore = new Socket(InetAddress.getLoopbackAddress(), port);
      OutputStream out = dStore.getOutputStream();
      PrintWriter printWriter = new PrintWriter(out, true);
      printWriter.println("REBALANCE_STORE " + fileName + " " + fileSize);
      dStoreLogger.info("REBALANCE_STORE -> " + "[" + cport + "]");
      fileToSend = fileName;
      writeFileContent(out);
    } catch (IOException e) {
      dStoreLogger.info("Error sending file to port: " + port);
    }
  }

  /**
   * Method to send file content
   * @param out
   */
  private synchronized static void writeFileContent(OutputStream out){
    dStoreLogger.info("Sending file content...");
    File file = new File(currentFolder + File.separator + fileToSend);
    try {
      byte[] data = Files.readAllBytes(file.toPath());
      out.write(data);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  // Getters and Setters
  public static int getCport() {
    return cport;
  }

  public static void setCport(int cport) {
    Dstore.cport = cport;
  }

  public static void setPort(int port){
    Dstore.currentPort = port;
  }

  public static String getCurrentFolder() {
    return currentFolder;
  }

  public static void setCurrentFolder(String currentFolder) {
    Dstore.currentFolder = currentFolder;
  }

  public static String getCurrentFileName() {
    return currentFileName;
  }

  public static void setCurrentFileName(String currentFileName) {
    Dstore.currentFileName = currentFileName;
  }

  public static int getCurrentFileSize() {
    return currentFileSize;
  }

  public static void setCurrentFileSize(int currentFileSize) {
    Dstore.currentFileSize = currentFileSize;
  }

  public static String getFileToSend() {
    return fileToSend;
  }
}
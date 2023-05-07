import java.io.File;
import java.io.IOException;
import java.util.Scanner;


public class ClientApp {

  private int cport;
  private int timeout;
  private File downloadFolder;
  private File uploadFolder;
  private Client client;

  public ClientApp(int cport, int timeout, File downloadFolder, File uploadFolder) {
    this.cport = cport;
    this.timeout = timeout;
    this.downloadFolder = downloadFolder;
    this.uploadFolder = uploadFolder;
  }

  public void start() throws IOException {
    client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
    client.connect();

    Scanner scanner = new Scanner(System.in);
    String command;

    while (true) {
      System.out.print(">>Enter command (STORE, LOAD, REMOVE, LIST, or QUIT): ");
      command = scanner.nextLine().trim();

      if (command.startsWith("STORE")) {
        String[] parts = command.split(" ");
        if (parts.length != 3) {
          System.out.println("Invalid command format. Usage: STORE <filename> <filesize>");
        } else {
          String filename = parts[1];
          int filesize = Integer.parseInt(parts[2]);

          // Load the file content into a byte array
          File file = new File(filename);
          try{
            client.store(file);
          } catch (Exception e){
            System.out.println("Error trying to store file " + filename);
          }
        }
      }
      else if (command.startsWith("LOAD")) {
        String[] parts = command.split(" ");
        if (parts.length != 2) {
          System.out.println("Invalid command format. Usage: LOAD <filename>");
        } else {
          String filename = parts[1];
          try {
            client.load(filename, downloadFolder);
          } catch (Exception e) {
            System.out.println("Error loading file " + filename);
          }
        }
      } else if (command.startsWith("REMOVE")) {
        String[] parts = command.split(" ");
        if (parts.length != 2) {
          System.out.println("Invalid command format. Usage: REMOVE <filename>");
        } else {
          String filename = parts[1];
          // Perform the remove operation here
          try{
            client.remove(filename);
          } catch (Exception e){
            System.out.println("Error removing file " + filename);
          }
        }
      } else if (command.equals("LIST")) {
        // Perform the list operation here
        try {
          System.out.println("Retrieving file list from server...");
          String[] fileList =  client.list();
          System.out.println(fileList.length + " files found on server.");
          for (String file : fileList) {
            System.out.println(">> "+file);
          }
        } catch (Exception e) {
          System.out.println("Error retrieving file list from server.");
        }

      } else if (command.equals("QUIT")) {
        client.disconnect();
        break;
      } else {
        System.out.println("Invalid command. Please enter a valid command.");
      }
    }

    client.disconnect();
    scanner.close();
  }

  public static void main(String[] args) throws Exception {
    int cport = Integer.parseInt(args[0]);
    int timeout = Integer.parseInt(args[1]);

    File downloadFolder = new File("downloads");
    if (!downloadFolder.exists())
      if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");

    File uploadFolder = new File("to_store");
    if (!uploadFolder.exists())
      if(!uploadFolder.mkdir()) throw new RuntimeException("to_store folder does not exist");

    ClientApp clientApp = new ClientApp(cport, timeout, downloadFolder, uploadFolder);
    clientApp.start();
  }
}

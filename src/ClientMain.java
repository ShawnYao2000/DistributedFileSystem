import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Random;

/**
 *  This is just an example of how to use the provided client library. You are expected to customise this class and/or 
 *  develop other client classes to test your Controller and Dstores thoroughly. You are not expected to include client 
 *  code in your submission.
 */
public class ClientMain {
	
	public static void main(String[] args) throws Exception{
		
		final int cport = Integer.parseInt(args[0]);
		int timeout = Integer.parseInt(args[1]);

                /*
        ClientMain.main(new String[]{"4000", "3000", "to_store", "to_load"});
        final int cport = 4000;
        final int timeout = 1000;
        final String to_store = "to_store";
        final String to_load = "to_load";
         */

    // this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
    File uploadFolder = new File(args[2]);
    if (!uploadFolder.exists())
      throw new RuntimeException("to_store folder does not exist");
		
		// this client expects a 'to_load' folder in the current directory; all files loaded from the store will be stored in this folder
		File downloadFolder = new File(args[3]);
		if (!downloadFolder.exists())
			if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");
		
		// launch a single client
		testClient(cport, timeout, downloadFolder, uploadFolder);
		
		// launch a number of concurrent clients, each doing the same operations
		for (int i = 0; i < 10; i++) {
			new Thread() {
				public void run() {
					test2Client(cport, timeout, downloadFolder, uploadFolder);
				}
			}.start();
		}

    //test(cport, timeout, downloadFolder, uploadFolder);
	}
	
	public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
			client.connect();
			Random random = new Random(System.currentTimeMillis() * System.nanoTime());
			
			File fileList[] = uploadFolder.listFiles();
			for (int i=0; i<fileList.length/2; i++) {
				File fileToStore = fileList[random.nextInt(fileList.length)];
				try {					
					client.store(fileToStore);
				} catch (FileAlreadyExistsException e) {
					System.out.println("Error storing file " + fileToStore);
          System.out.println("File already exists in the store");
				} catch (Exception e) {
          System.out.println("Error storing file " + fileToStore);
          e.printStackTrace();
        }
			}
			
			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			for (int i = 0; i < list.length/4; i++) {
				String fileToRemove = list[random.nextInt(list.length)];
				try {
					client.remove(fileToRemove);
				} catch (Exception e) {
					System.out.println("Error remove file " + fileToRemove);
					e.printStackTrace();
				}
			}
			
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

	public static void testClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {
			
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
		
			try { client.connect(); } catch(IOException e) { e.printStackTrace(); System.out.println("Unable to connect on port: "+cport); return; }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
			// store first file in the to_store folder twice, then store second file in the to_store folder once
			File fileList[] = uploadFolder.listFiles();
			if (fileList.length > 0) {
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }				
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			}
			if (fileList.length > 1) {
				try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
			}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			if (list != null && list.length > 0)
				try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

	public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
		System.out.println("Retrieving list of files...");
		String list[] = client.list();
		
		System.out.println("Ok, " + list.length + " files:");
		int i = 0; 
		for (String filename : list)
			System.out.println("[" + i++ + "] " + filename);
		
		return list;
	}

  public static void test(int cport, int timeout, File downloadFolder, File uploadFolder) {
    Client client = null;

    File fileList[] = uploadFolder.listFiles();
    for (int i=0; i<fileList.length; i++) {
      System.out.println("File " + i + ": " + fileList[i].getName());
    }
    try {
      client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
      client.connect();
      for (int i=0; i<fileList.length; i++) {
        try{
          client.store(fileList[i]);
        } catch (FileAlreadyExistsException e) {
          System.out.println("Error storing file " + fileList[i]);
          System.out.println("File already exists in the store");
        } catch (Exception e) {
          System.out.println("Error storing file " + fileList[i]);
          e.printStackTrace();
        }
      }} catch(IOException e) { e.printStackTrace(); }
  }
	
}

import java.util.ArrayList;
import java.util.HashMap;

public class Index {

  ArrayList<String> files;
  HashMap<String, String> fileStats;

  /**
   * Constructor for Index
   * @param files
   * @param fileStats
   */
  public Index(ArrayList<String> files, HashMap<String, String> fileStats) {
    this.files = files;
    this.fileStats = fileStats;
  }

  /**
   * Getter for files
   * @return files
   */
  public ArrayList<String> getFiles() {
    return files;
  }

  /**
   * Setter for files
   * @param files
   */
  public void setFiles(ArrayList<String> files) {
    this.files = files;
  }

  /**
   * Getter for fileStats
   * @return fileStats
   */
  public HashMap<String, String> getFileStats() {
    return fileStats;
  }

  /**
   * Setter for fileStats
   * @param fileStats
   */
  public void setFileStats(HashMap<String, String> fileStats) {
    this.fileStats = fileStats;
  }

}

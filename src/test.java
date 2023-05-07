class Con {

  public static void main(String[] args) {
    Controller.main(new String[]{"4000", "2", "3000", "11000"});
  }
          /*
        final int cport = 4000;
        final int rep_factor = 1;
        setRep_factor(rep_factor);
        final int timeout = 2000;
        final int reb_period = 1000;
         */
}

class cli{
  public static void main(String[] args) throws Exception {
    ClientMain.main(new String[]{"4000", "1000", "to_store", "to_load"});
            /*
        ClientMain.main(new String[]{"4000", "3000", "to_store", "to_load"});
        final int cport = 4000;
        final int timeout = 1000;
        final String to_store = "to_store";
        final String to_load = "to_load";
         */
  }
}

class ds {
  public static void main(String[] args) {
    Dstore.main(new String[]{"5000", "4000", "11000", "DStore1"});
            /*
        final int port = 5000;
        final int cport = 4000;
        setCport(cport);
        final int timeout = 11000;
        final String file_folder = "DStore1";
        setFile_folder(file_folder);
         */
  }
}

class ds2 {
  public static void main(String[] args) {
    Dstore.main(new String[]{"5001", "4000", "11000", "DStore2"});
            /*
        final int port = 5001;
        final int cport = 4000;
        setCport(cport);
        final int timeout = 11000;
        final String file_folder = "DStore2";
        setFile_folder(file_folder);
         */
  }
}

class ds3 {
  public static void main(String[] args) {
    Dstore.main(new String[]{"5002", "4000", "11000", "DStore3"});
            /*
        final int port = 5002;
        final int cport = 4000;
        setCport(cport);
        final int timeout = 11000;
        final String file_folder = "DStore3";
        setFile_folder(file_folder);
         */
  }
}
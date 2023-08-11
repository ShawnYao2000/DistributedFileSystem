### Academic Integrity Disclaimer
This repository does not contains the source code for the project. Due to academic integrity restrictions, we are only providing the Unix executable files for this project. We strictly prohibit any direct copying or plagiarism of our code. However, we welcome individuals to explore and learn from the code to understand its workings. Please note that we do not assume any responsibility if individuals choose to copy our code for their own purposes.

### Description

- A distributed file storage system that can handle concurrent client requests using TCP Sockets.
- For simplicity, the processes all occur on the same machine, running on different ports.

### Network Structure

- The **Distributed File Storage System** is made-up of three components:
  - **Controller** : The server within the system - takes requests from clients and handles them accordingly.
  - **Dstore** : A data store unit that is connected to the system via the controller. Dstores recieve commands from the controller and store files accordingly.
  - **Client** : A client that makes use of the system. The client sends requests to the Controller which are handeled accordingly.

## Usage

- Commands are input via the **command line interface** on the **Client process**.
- Any file can be referenced as a **relative** or **absolute path**, and must include the **file extension**.

### STORE

- The **STORE** command has the following syntax:

```assembly
STORE <filename> <filesize>
```

- Where:
  - `filename` : The **path** to the file to be stored.
  - `filesize` : The **size** of the file in bytes.


### LOAD

- The **LOAD** command has the following syntax:

```assembly
LOAD <filename>
```

- Where:
  - `filename` : The **name** of the file being loaded.


### LIST

- The **LIST** command has the following syntax:

```assembly
LIST
```


### REMOVE

- The **REMOVE** command has the following syntax:

```assembly
REMOVE <filename>
```

- Where:
  - `filename` : The **name** of the file to be removed.


---

### Logging

- Different pieces of information will be recorded in the terminal of every component (Controller, Client, Dstore) as the system is running and handling requests.

# PIQNIC: a P2p system for Query processiNg over semantIC data
A github repository for PIQNIC Version 0.1-SNAPSHOT

This Readme and repository are works in progress. The PIQNIC version available here is the one used for experiments in our paper (see also our website at http://relweb.cs.aau.dk/piqnic/).

PIQNIC is currently implemented only as a prototype for testing the performance and availability of a network. Some features, described in the paper are therefore missing. Here is a list of known differences:

    Missing/low-efficient implementation for things not essential for performance, e.g. joining the network and adding new fragments to the network.
    The random part of the neighbors are not random, since they are the least related neighbors of other nodes.
    No checks for insertion of fragments, and if it is possible. The request is passed on if an exception is thrown.
    Request identifiers are used to avoid responding to the same request twice instead of fragment identifier used to not answer the same fragment twice.
    The query exection strategies are in the code called flood (single), bind (bulk) and down (full).


### Installation
1. Download the sources and unpack.
2. Use `mvn install` to generate jar file
3. Run with the following command: java -jar [filename].jar [config].json:
4. Connect to the CLI port and follow the steps on screen.
5. Example config.json:
```
   {
     "replication" : 4,
     "ports" : {
       "listener" : 7625,
       "cli" : 7626,
       "test" : 7627
     "maxFragments" : 200,
     "timeToLive" : 5,
     "shuffleLength" : 4,
     "neighbours" : 5,
     "minutesTilShuffle" : 200,
     "maxDelay" : 500,
     "version" : "0.1-SNAPSHOT"
   }
   ```
   
### Test Startup
We have included the ability to quickly setup a test-environment. The requirements for this, however, is that the dataset is already distributed to the clients in the following directory format (the dataet and distribution we used can be downloaded on our website at https://relweb.cs.aau.dk/piqnic/):

  -  dataset1
      - fragment1.hdt
      - fragment2.hdt
      - fragment3.hdt
      - fragments
    - dataset2
      - fragment1.hdt
      - fragment2.hdt
      - fragment3.hdt
      - fragments
    - dataset_distro

The file `dataset_distro` contains each dataset and the client it has been distributed to, in the following format: `dataset1;125`, means that dataset1 has been assigned to peer 125, and is owned by that. One line per dataset (keep the file names as fragments and dataset_distro). Index files are optional for HDT files.

The clients can then be started with the included `test_startup.sh` file like so:

```
./test_startup.sh [filename].jar [config].json [No. of Clients] [No. of Neighbors] [Replication Factor] [/path/to/datasets/]
```

Each fragment directory has the `fragments` file, containing the URI of the predicates in the fragment and its identifier, e.g. `http://www.w3.org/1999/02/22-rdf-syntax-ns#type;fragment1`

---   
Any questions, feel free to contact Christian Aebeloe at caebel@cs.aau.dk

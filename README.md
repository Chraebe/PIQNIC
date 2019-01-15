# PIQNIC: a P2p system for Query processiNg over semantIC data
A github repository for PIQNIC Version 0.1-SNAPSHOT

This Readme and repository are works in progress. The PIQNIC version available here is the one used for experiments in our paper (see also our website at http://qweb.cs.aau.dk/piqnic/).

###Installation
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
Any questions, feel free to contact Christian Aebeloe at caebel@cs.aau.dk
# CloudComputingProject
Steps to execute:
1. set up google cloud computing account
2. follow gcp guide
3. get the external ip address for the master node
4. use winscp to get access to file system
5. (may need to generate public/private key and place private key on ssh keys in gcp)
6. guide here:https://winscp.net/eng/docs/guide_google_compute_engine
7. when logging in, click on advanced, then Authentication and add private key
8. add ProjectTestData to the main directory
9. Add the bin files for CloudComputingProject to a new directory called InvertedIndexing
10. repeat for CloudComputingSearch by adding bin files to directory called SearchTerm
11. repeat for CloudComputingTopN by adding bin files to directory called TopN
12. repeat for CloudComputingSrc by adding bin files to the main directory itself
13. also add hadoop.sh, hadoopSearch.sh, and hadoopTopN.sh to the main dir
14. execute java RunHadoopServer in the gcp command prompt
15. execute java UserApp <gcp master node ip> 8765...on local machine
16. should connect, but if not, probably hit a firewall
17. execute curl ifconfig.me to get IP of local machine
18. go to gcp website, go to master machine
19. edit the network, and add the machines IP to the allowed incoming traffic in 
    the firewall rules
20. repeat 15
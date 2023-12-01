#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/stat.h>

#define RESOURCE_SERVER_PORT 1055 // Change this!
#define BUF_SIZE 256

// We make this a global so that we can refer to it in our signal handler
int serverSocket;

pthread_mutex_t lock;

// Struct to hold the information to pass to the threads to partition the text to the disks
struct PartitionInfo {
    char rawPartitionPath[50];
    char filename[20];
    char text[256];
    int stringIndex;
    int count;
    int disk;
    int partitions;
};

/*
 We need to make sure we close the connection on signal received, otherwise we have to wait
 for server to timeout.
 */
void closeConnection() {
    printf("\nClosing Connection with file descriptor: %d \n", serverSocket);
    close(serverSocket);
    exit(1);
}

// Thread partitioning function to round robin words to the files on the disks
void * partitionWithThreads(void * arg) {

    struct PartitionInfo * partitionInfo = ((struct PartitionInfo *)arg);

    char partitionPath[100];
    char word[20];
    int wordsRead;

    // TODO: Look at the mutex, could it be better?
    pthread_mutex_lock(&lock);

    while ((wordsRead = sscanf(partitionInfo->text + partitionInfo->stringIndex, "%19s", word)) > 0) {
        // Determine what disk the word should go to
        partitionInfo->disk = partitionInfo->count % partitionInfo->partitions;

        printf("Word %d: %s\n", partitionInfo->stringIndex + 1, word);
        // Make the filepath for the partition
        sprintf(partitionPath, "%s%d/%s", partitionInfo->rawPartitionPath, partitionInfo->disk, partitionInfo->filename);
        printf("\tgoing to path: %s\n", partitionPath);

        FILE *outputStream = fopen(partitionPath, "a");

        if (outputStream != NULL) {
            fprintf(outputStream, "%s\n", word);
            fclose(outputStream);
        } else {
            printf("Error opening file");
        }
        partitionInfo->count++;

        // Increment stringIndex and get rid of spaces for reading next word
        partitionInfo->stringIndex += strlen(word);
        while (partitionInfo->text[partitionInfo->stringIndex] == ' ') {
            partitionInfo->stringIndex++;
        }
    }

    pthread_mutex_unlock(&lock);
}

void createFile(char *information) {
    printf("We are creating a mapping file here "); //DELETE

    // Breaking up the create request from the user into seperate variables
    char filename[20];
    int partitions;
    char text[256];
    sscanf(information, "%s %d %[^\n]", filename, &partitions, text);

    // Making the path for the mapping file and adding the filename
    char mappingPath[100];
    strcpy(mappingPath, "/home/stu/jbuxton21/FinalProject/mappings/");
    strcat(mappingPath, filename);
    printf("%s\n", mappingPath);

    // Open mapping file for the given filename
    FILE *outputStream = fopen(mappingPath, "w");

    // Write each of the partion paths to the mapping file and create the disks if they don't exist
    int i;
    char rawDiskPath[50];
    char completeDiskPath[100];
    strcpy(rawDiskPath, "/home/stu/jbuxton21/FinalProject/disks/disk");
    for(i=0; i < partitions; i++){
        sprintf(completeDiskPath, "%s%d", rawDiskPath, i);
        fprintf(outputStream, "%d:%s/%s\n", i, completeDiskPath, filename);
        mkdir(completeDiskPath, 0755);
    }

    // Close mapping file
    fclose(outputStream);

    // Create struct to give to the threads for all the information to partition string of text
    struct PartitionInfo partitionInfo;

    strcpy(partitionInfo.rawPartitionPath, rawDiskPath);
    strcpy(partitionInfo.filename, filename);
    strcpy(partitionInfo.text, text);
    partitionInfo.stringIndex = 0;
    partitionInfo.count = 0;
    partitionInfo.disk = 0;
    partitionInfo.partitions = partitions;

    // Creating the Threads and Mutex
    pthread_t partitionThreads[partitions];
    pthread_mutex_init(&lock, NULL);

    for (int i = 0; i < partitions; ++i) {
        if (pthread_create(&partitionThreads[i], NULL, partitionWithThreads, (void *)&partitionInfo) != 0) {
            printf("Error creating thread %d\n", i);
        }
    }

    // Waiting for all the Threads to join
    for (int i = 0; i < partitions; ++i) {
        pthread_join(partitionThreads[i], NULL);
        printf("Thread %d joined\n", i);
    }

    printf("All threads have finished\n");

}

void readFile() {



    printf("We are reading a file here\n");
}

void deleteFile(char * file) {
    const char filePath[100] = "/home/stu/jbuxton21/FinalProject/disks/disk";
    int check = 1;
    int i = 0;

    while (check == 1) {
        char path[115];
        char mappings[115] = "/home/stu/jbuxton21/FinalProject/mappings/";

        sprintf(path, "%s%d", filePath, i);
        strcat(path, "/");
        strcat(path, file);

        strcat(mappings, file);
        if (access(path, F_OK) == 0) {
            remove(mappings);
        }
        else {
            printf("Mapping doesn't exist\n");
        }

        if (access(path, F_OK) == 0) {
            remove(path);
            i++;
        }
        else {
            check = 0;
        }
    }

    printf("We are deleting a file here\n");
}


// Create a separate function to process thread request
void * processClientRequest(void * request) {
    int connectionToClient = *(int *)request;
    char receiveLine[BUF_SIZE];
    char sendLine[BUF_SIZE];

    read(connectionToClient, receiveLine, BUF_SIZE);

    printf("We got: %s\n", receiveLine); //DELETE

    char command[10];
    char information[256];
    sscanf(receiveLine, "%s %[^\n]", command, information);

    printf("Command: %s\n", command); //DELETE
    printf("information: %s\n", information);
    if ( strcmp(command, "create") == 0){
        createFile(information);
    }
    else if( strcmp(command, "read") == 0){
        readFile();
    }
    else if ( strcmp(command, "delete") == 0){
        deleteFile(information);
    }
    else {
        printf("Invalid commnad\n");
    }


    bzero(&receiveLine, sizeof(receiveLine));
    close(connectionToClient);



/*

    int bytesReadFromClient = 0;
    // Read the request from the client
    while ( (bytesReadFromClient = read(connectionToClient, receiveLine, BUF_SIZE)) > 0) {
        // Need to put a NULL string terminator at end
        receiveLine[bytesReadFromClient] = 0;

        // Show what client sent
        printf("Received: %s\n", receiveLine);

        // Print text out to buffer, and then write it to client (connfd)
        snprintf(sendLine, sizeof(sendLine), "true");

        printf("%s Sending s\n", sendLine);
        write(connectionToClient, sendLine, strlen(sendLine));

        // Zero out the receive line so we do not get artifacts from before
        bzero(&receiveLine, sizeof(receiveLine));
        close(connectionToClient);
    }*/


}

int main(int argc, char *argv[]) {
    int connectionToClient, bytesReadFromClient;

    // Create a server socket
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serverAddress;
    bzero(&serverAddress, sizeof(serverAddress));
    serverAddress.sin_family      = AF_INET;

    // INADDR_ANY means we will listen to any address
    // htonl and htons converts address/ports to network formats
    serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);
    serverAddress.sin_port        = htons(RESOURCE_SERVER_PORT);

    // Bind to port
    if (bind(serverSocket, (struct sockaddr *) &serverAddress, sizeof(serverAddress)) == -1) {
        printf("Unable to bind to port just yet, perhaps the connection has to be timed out\n");
        exit(-1);
    }

    // Before we listen, register for Ctrl+C being sent so we can close our connection
    struct sigaction sigIntHandler;
    sigIntHandler.sa_handler = closeConnection;
    sigIntHandler.sa_flags = 0;

    sigaction(SIGINT, &sigIntHandler, NULL);

    // Listen and queue up to 10 connections
    listen(serverSocket, 10);

    while (1) {
        /*
         Accept connection (this is blocking)
         2nd parameter you can specify connection
         3rd parameter is for socket length
         */
        connectionToClient = accept(serverSocket, (struct sockaddr *) NULL, NULL);

        // Kick off a thread to process request
        pthread_t someThread;
        pthread_create(&someThread, NULL, processClientRequest, (void *)&connectionToClient);

    }
}
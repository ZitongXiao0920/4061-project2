#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>

int main() {
    int msgid;

    // Iterate through all possible message queue IDs
    for (msgid = 0; ; msgid++) {
        // Get information about the message queue
        struct msqid_ds buf;
        if (msgctl(msgid, IPC_STAT, &buf) == -1) {
            // If msgctl returns -1, it means no more message queues exist
            break;
        }

        // Remove the message queue
        if (msgctl(msgid, IPC_RMID, NULL) == -1) {
            perror("msgctl IPC_RMID failed");
            // Handle error if necessary
        } else {
            printf("Removed message queue with ID: %d\n", msgid);
        }
    }

    return 0;
}

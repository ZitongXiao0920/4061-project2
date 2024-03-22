#include "utils.h"

pid_t *workers;          // Workers determined by batch size
int *worker_done;        // 1 for done, 0 for still running

// Stores the results of the autograder (see utils.h for details)
autograder_results_t *results;

int num_executables;      // Number of executables in test directory
int total_params;         // Total number of parameters to test - (argc - 2)
int num_workers;          // Number of workers to spawn



void launch_worker(int msqid, int pairs_per_worker, int worker_id) {
    
    pid_t pid = fork();

    // Child process
    if (pid == 0) {
        printf("the worker parameters are %d and %d\n",msqid,worker_id);
        // TODO: exec() the worker program and pass it the message queue id and worker id.
        //       Use ./worker as the path to the worker program.
        char msqid_str[20];  // Adjust size as needed
        char worker_id_str[20];  // Adjust size as needed
        snprintf(msqid_str, sizeof(msqid_str), "%d", msqid);
        snprintf(worker_id_str, sizeof(worker_id_str), "%d", worker_id);
        
        // Execute the worker program and pass message queue id and worker id as arguments
        execl("./worker", "worker", msqid_str, worker_id_str, (char *)NULL);
        if (msgctl(msqid, IPC_RMID, NULL) == -1) {
            perror("msgctl (remove queue) failed");
            exit(1);
        }
        perror("Failed to spawn worker");
        exit(1);
    } 
    // Parent process
    else if (pid > 0) {
        // TODO: Send the total number of pairs to worker via message queue (mtype = worker_id)
        msgbuf_t msg;
        msg.mtype = worker_id;
        sprintf(msg.mtext, "%d", pairs_per_worker);
        // strncpy(msg.mtext, pairs_per_worker, sizeof(msg.mtext) - 1);
        if (msgsnd(msqid, &msg, sizeof(msg.mtext), 0) == -1) {
            perror("msgsnd failed");
            // handle the error, possibly with a continue or break statement
        }
        printf("sending the message %ld and %s and pid %d\n",msg.mtype,msg.mtext,pid);

        // Store the worker's pid for monitoring
        workers[worker_id - 1] = pid;
    }
    // Fork failed 
    else {
        perror("Failed to fork worker");
        exit(1);
    }
}


// TODO: Receive ACK from all workers using message queue (mtype = BROADCAST_MTYPE)
void receive_ack_from_workers(int msqid, int num_workers) {
    for(int i =0; i<num_workers;i++){
        msgbuf_t msg;
        if (msgrcv(msqid, &msg, sizeof(msg.mtext),BROADCAST_MTYPE,0 ) == -1) {
            perror("msgrcv failed");
            exit(1);
        }
        printf("receive_ack: %ld %s and the index of workers:%d the number %d\n", msg.mtype,msg.mtext,i,num_workers);
    }
    printf("after received ack from workers!!!!!!!!!\n");
}

// TODO: Send SYNACK to all workers using message queue (mtype = BROADCAST_MTYPE)
void send_synack_to_workers(int msqid, int num_workers) {
    for(int i =0; i<num_workers;i++){
        msgbuf_t msg;
        msg.mtype = BROADCAST_MTYPE;
        sprintf(msg.mtext, "%s", "1");
        // strncpy(msg.mtext, pairs_per_worker, sizeof(msg.mtext) - 1);
        if (msgsnd(msqid, &msg, sizeof(msg.mtext), 0) == -1) {
            perror("msgsnd failed");
            // handle the error, possibly with a continue or break statement
        }
        printf("sending the message %ld and %s and worker index %d\n",msg.mtype,msg.mtext,i);
    }
    
}


// Wait for all workers to finish and collect their results from message queue
void wait_for_workers(int msqid, int pairs_to_test, char **argv_params, char** executable_paths) {
    int received = 0;
    worker_done = malloc(num_workers * sizeof(int));
    for (int i = 0; i < num_workers; i++) {
        worker_done[i] = 0;
    }

    while (received < pairs_to_test) {
        for (int i = 0; i < num_workers; i++) {
            if (worker_done[i] == 1) {
                continue;
            }

            // Check if worker has finished
            pid_t retpid = waitpid(workers[i], NULL, WNOHANG);
            
            int msgflg;
            if (retpid > 0)
                // Worker has finished and still has messages to receive
                msgflg = 0;
            else if (retpid == 0)
                // Worker is still running -> receive intermediate results
                msgflg = IPC_NOWAIT;
            else {
                // Error
                perror("Failed to wait for child process");
                exit(1);
            }
            int m =0;
            // TODO: Receive results from worker and store them in the results struct.
            //       If message is "DONE", set worker_done[i] to 1 and break out of loop.
            //       Messages will have the format ("%s %d %d", executable_path, parameter, status)
            //       so consider using sscanf() to parse the message.
            while (1) {
                msgbuf_t msg;
                // fprintf(stderr, "before Received result the workid is %d\n", i+1);
                if (msgrcv(msqid, &msg, sizeof(msg.mtext), i+1 , msgflg) == -1) {
                    // perror("msgrcv failed");
                    // fprintf(stderr, "Failed to receive message for workid %d\n", i+1);
                    continue;
                }
                // while (msgrcv(msqid, &msg, sizeof(msg.mtext), 1, IPC_NOWAIT) != -1) {
                //     printf("while msgrcv Received message: %s\n", msg.mtext);
                // }

                fprintf(stderr, "after Received result the workid is %d\n", i+1);
                fprintf(stderr, "Received result: %ld %s\n", msg.mtype,msg.mtext);
                if (strncmp(msg.mtext, "DONE", 4) == 0) {
                    // msg.mtext starts with "DONE"
                    int pairs_each_worker=0;
                    char result[10];
                    fprintf(stderr, "Done received at grader\n");
                    sscanf(msg.mtext, "%s %d", result , &pairs_each_worker);
                    worker_done[i] = 1;
                    received += pairs_each_worker;
                    fprintf(stderr, "The received and pairs_to_test: %d %d\n", received, pairs_to_test);
                    break; // Exit the while loop after processing a "DONE" message
                }
                char executable_path[100];
                int parameter, status;
                sscanf(msg.mtext, "%s %d %d", executable_path, &parameter, &status);
                for(int l=0;l<num_executables;l++){//find the executable and update the paramter and status
                    if (strcmp(executable_path, executable_paths[l]) == 0) {
                        results[l].params_tested[m] = parameter;
                        results[l].status[m] = status;
                    }
                }
                m++;
                
            }
        }
    }

    free(worker_done);
}


int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <testdir> <p1> <p2> ... <pn>\n", argv[0]);
        return 1;
    }

    char *testdir = argv[1];
    total_params = argc - 2;

    char **executable_paths = get_student_executables(testdir, &num_executables);

    // Construct summary struct
    results = malloc(num_executables * sizeof(autograder_results_t));
    for (int i = 0; i < num_executables; i++) {
        results[i].exe_path = executable_paths[i];
        results[i].params_tested = malloc((total_params) * sizeof(int));
        results[i].status = malloc((total_params) * sizeof(int));
    }

    num_workers = get_batch_size();
    // Check if some workers won't be used -> don't spawn them
    if (num_workers > num_executables * total_params) {
        num_workers = num_executables * total_params;
    }
    workers = malloc(num_workers * sizeof(pid_t));

    // Create a unique key for message queue
    key_t key = IPC_PRIVATE;

    // TODO: Create a message queue
    int msqid;
    int msgflg = IPC_CREAT | 0666;

    msqid = msgget(key, msgflg);
    if (msqid == -1) {
        perror("Failed to create message queue");
        exit(1);
    }

    printf("the msgid at the parent is %d\n",msqid);

    int num_pairs_to_test = num_executables * total_params;
    
    // Spawn workers and send them the total number of (executable, parameter) pairs they will test
    for (int i = 0; i < num_workers; i++) {
        int leftover = num_pairs_to_test % num_workers - i > 0 ? 1 : 0;
        int pairs_per_worker = num_pairs_to_test / num_workers + leftover;

        // TODO: Spawn worker and send it the number of pairs it will test via message queue
        launch_worker(msqid, pairs_per_worker, i + 1);
    }

    // Send (executable, parameter) pairs to workers
    int sent = 0;
    for (int i = 0; i < total_params; i++) {
        for (int j = 0; j < num_executables; j++) {
            msgbuf_t msg;
            long worker_id = sent % num_workers + 1;
            
            // TODO: Send (executable, parameter) pair to worker via message queue (mtype = worker_id)
            msg.mtype = worker_id;
            sprintf(msg.mtext, "%s %s", executable_paths[j],argv[i+2]);
            // strncpy(msg.mtext, pairs_per_worker, sizeof(msg.mtext) - 1);
            if (msgsnd(msqid, &msg, sizeof(msg.mtext), 0) == -1) {
                perror("msgsnd failed");
                // handle the error, possibly with a continue or break statement
            }
            printf("sending the message %ld and %s and worker id %ld\n",msg.mtype,msg.mtext,worker_id);

            sent++;
        }
    }

    // TODO: Wait for ACK from workers to tell all workers to start testing (synchronization)
    receive_ack_from_workers(msqid, num_workers);

    // TODO: Send message to workers to allow them to start testing
    send_synack_to_workers(msqid, num_workers);
    
    // TODO: Wait for all workers to finish and collect their results from message queue
    wait_for_workers(msqid, num_pairs_to_test, argv + 2, executable_paths);

    // TODO: Remove ALL output files (output/<executable>.<input>)
    remove_output_files_mq(num_executables,argv+2,total_params);

    write_results_to_file(results, num_executables, total_params);

    // You can use this to debug your scores function
    // get_score("results.txt", results[0].exe_path);

    // Print each score to scores.txt
    write_scores_to_file(results, num_executables, "results.txt");

    // TODO: Remove the message queue
    if (msgctl(msqid, IPC_RMID, NULL) == -1) {
        perror("msgctl (remove queue) failed");
        exit(1);
    }

    printf("before free the result\n");
    // Free the results struct and its fields
    for (int i = 0; i < num_executables; i++) {
        free(results[i].exe_path);
        free(results[i].params_tested);
        free(results[i].status);
    }

    free(results);
    free(executable_paths);
    free(workers);
    
    return 0;
}
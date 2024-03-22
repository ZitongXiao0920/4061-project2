#include "utils.h"

// Run the (executable, parameter) pairs in batches of 8 to avoid timeouts due to 
// having too many child processes running at once
#define PAIRS_BATCH_SIZE 8

typedef struct {
    char *executable_path;
    int parameter;
    int status;
} pairs_t;

// Store the pairs tested by this worker and the results
pairs_t *pairs;

// Information about the child processes and their results
pid_t *pids;
int *child_status;     // Contains status of child processes (-1 for done, 1 for still running)

int curr_batch_size;   // At most PAIRS_BATCH_SIZE (executable, parameter) pairs will be run at once
long worker_id;        // Used for sending/receiving messages from the message queue
struct itimerval timers[30];
struct sigaction sa;
int pids_size;
int next_child_to_kill = 0;

// TODO: Timeout handler for alarm signal - should be the same as the one in autograder.c
void timeout_handler(int signum) {
    if (signum == SIGALRM) {
        // Check if the next child to kill is valid and running
        if (child_status[next_child_to_kill] == 1) {
            printf("Child process %d is running too long. Terminating...\n", pids[next_child_to_kill]);
            kill(pids[next_child_to_kill], SIGKILL); // Terminate the child process
            pids[next_child_to_kill] = 0; // Reset the pid entry
        }
        next_child_to_kill++; // Move to the next child in the array
        PrepareTimer(sa, timers[next_child_to_kill-1], TIMEOUT_SECS); // Reset the timer
    }
    
}

void PrepareTimer(struct sigaction sa, struct itimerval timer, int time){
    sa.sa_handler = timeout_handler;

    // Now set up the sigaction for SIGALRM
    if (sigaction(SIGALRM, &sa, NULL) < 0) {
        perror("sigaction");
    }

    // Configure the timer to expire after TIMEOUT_SECS seconds
    timer.it_value.tv_sec = time; // First timer expiration in seconds
    timer.it_value.tv_usec = 0;           // First timer expiration in microseconds

    // Setting it_interval to 0,0 means no subsequent timer expirations after the first
    timer.it_interval.tv_sec = time;         // Following timer expirations in seconds
    timer.it_interval.tv_usec = 0;        // Following timer expirations in microseconds

    // Set the ITIMER_REAL timer with the interval we just prepared
    if (setitimer(ITIMER_REAL, &timer, NULL) < 0) {
        perror("setitimer");
    }
}

// Execute the student's executable using exec()
void execute_solution(char *executable_path, int param, int batch_idx) {
 
    pid_t pid = fork();

    // Child process
    if (pid == 0) {
        char *executable_name = get_exe_name(executable_path);
        char input[10];
        // TODO: Redirect STDOUT to output/<executable>.<input> file
        char filename[100];
        snprintf(filename, sizeof(filename), "output/%s.%d", executable_name, param);
        snprintf(input, sizeof(input), "%d", param);
        int output_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if(output_fd<0){
            printf("fail to open the executable\n");
            exit(1);
        }

        if(dup2(output_fd,1)<0){
            printf("fail to redirect stdout to file\n");
            close(output_fd);
            exit(1);
        }

        // TODO: Input to child program can be handled as in the EXEC case (see template.c)
        printf("the executable path is %s, the name is %s and the input %d\n",executable_path,executable_name,param);
        execl(executable_path,executable_name,input,(char *)NULL);

        fprintf(stderr, "Failed to execute program in worker: %s\n", strerror(errno));
        exit(1);
    }
    // Parent process
    else if (pid > 0) {
        pids[batch_idx] = pid;
    }
    // Fork failed
    else {
        perror("Failed to fork");
        exit(1);
    }
}


// Wait for the batch to finish and check results
void monitor_and_evaluate_solutions(int finished) {
    // Keep track of finished processes for alarm handler
    child_status = malloc(curr_batch_size * sizeof(int));
    for (int j = 0; j < curr_batch_size; j++) {
        child_status[j] = 1;
    }

    // MAIN EVALUATION LOOP: Wait until each process has finished or timed out
    for (int j = 0; j < curr_batch_size; j++) {
        char *current_exe_path = pairs[finished + j].executable_path;
        int current_param = pairs[finished + j].parameter;

        char filename[100]; // Adjust size as needed
        FILE *file;
        char line[256]; // Adjust size based on expected line length
        int status;
        pid_t pid = waitpid(pids[j], &status, 0);

        // TODO: What if waitpid is interrupted by a signal?
        while(pid==-1 && errno == EINTR){
            pid = waitpid(pids[j], &status, 0);
        }


        int exit_status = WEXITSTATUS(status);
        int exited = WIFEXITED(status);
        int signaled = WIFSIGNALED(status);

        // TODO: Check if the process finished normally, segfaulted, or timed out and update the 
        //       pairs array with the results. Use the macros defined in the enum in utils.h for 
        //       the status field of the pairs_t struct (e.g. CORRECT, INCORRECT, SEGFAULT, etc.)
        //       This should be the same as the evaluation in autograder.c, just updating `pairs` 
        //       instead of `results`.
        pairs[finished + j].status = STUCK_OR_INFINITE;
        if (pid>0){
            
            // Process has changed state
            if (exited) {
                fprintf(stderr, "inside exited.\n");
                sprintf(filename, "output/sol_%d.%d", finished + j + 1, current_param);
                fprintf(stderr, "the filename is %s.\n",filename);
                FILE *file = fopen(filename, "r");
                if (file == NULL) {
                    perror("Failed to open file");
                    exit(1); // or handle the error as appropriate
                }
                if (fgets(line, sizeof(line), file) == NULL || fgets(line, sizeof(line), file) == NULL) {
                        fprintf(stderr, "Error or end of file reached before reading the second line.\n");
                    } else {
                        fprintf(stderr, "The result is %s.\n",line);
                    }
                fprintf(stderr, "after get line.\n");   
                if (line == 0) {
                    fprintf(stderr, "side correct  the pid is %d and the batch is %d and the status is %d \n", pid, curr_batch_size,status);
                    pairs[finished + j].status = CORRECT;
                } else{
                    fprintf(stderr, "side incorrectthe pid is %d and the batch is %d and the status is %d \n", pid, curr_batch_size,status);
                    pairs[finished + j].status = INCORRECT;
                }
            } else if (signaled) {
                if (WTERMSIG(status) == SIGSEGV) {
                    fprintf(stderr, "side segfault the pid is %d and the batch is %d and the status is %d \n",  pid, curr_batch_size,status);
                    pairs[finished + j].status = SEGFAULT;

                    // next_child_to_kill++;
                }
                // Include additional checks for other signals if necessary
            }

            
        }


        // Mark the process as finished
        child_status[j] = -1;
    }

    free(child_status);
}


// Send results for the current batch back to the autograder
void send_results(int msqid, long mtype, int finished,int curr_batch_size) {
    fprintf(stderr, "inside send_result\n");
    // Format of message should be ("%s %d %d", executable_path, parameter, status)
    for (int i = finished; i < finished+curr_batch_size; i++) {
        msgbuf_t msg;
        msg.mtype = mtype;
        // fprintf(stderr, "after my type\n");
        // fprintf(stderr, "Executable Path: %s\n", pairs[i].executable_path);
        // fprintf(stderr, "Parameter: %d\n", pairs[i].parameter);
        // fprintf(stderr, "Status: %d\n", pairs[i].status);

        sprintf(msg.mtext, "%s %d %d", pairs[i].executable_path, pairs[i].parameter, pairs[i].status);
        fprintf(stderr, "Formatted Message: %s with work_id = %ld\n", msg.mtext,msg.mtype);
        if (msgsnd(msqid, &msg, sizeof(msg.mtext), 0) == -1) {
            perror("msgsnd failed");
            // handle the error, possibly with a continue or break statement
        }
        // if (msgrcv(msqid, &msg, sizeof(msg.mtext), 1, 0) != -1) {
        //     printf("!!!!!!!!!!!!!!!receive Received message: %s\n", msg.mtext);
        // }
    }
}


// Send DONE message to autograder to indicate that the worker has finished testing
void send_done_msg(int msqid, long mtype,int pairs_to_test) {
    msgbuf_t msg;
    msg.mtype = mtype;
    sprintf(msg.mtext, "%s %d", "DONE",pairs_to_test); //1 for done, 0 for still running
    // strncpy(msg.mtext, pairs_per_worker, sizeof(msg.mtext) - 1);
    fprintf(stderr, "Sending the done result from worker\n");
    if (msgsnd(msqid, &msg, sizeof(msg.mtext), 0) == -1) {
        perror("msgsnd failed");
        // handle the error, possibly with a continue or break statement
    }

}

void receive_pairs_int(int worker_id, int* pairs_to_test,int msqid){
    msgbuf_t msg;
    if (msgrcv(msqid, &msg, sizeof(msg.mtext), worker_id,0 ) == -1) {
        perror("msgrcv failed");
        exit(1);
    }
    printf("Received message at pairs: %ld %s\n", msg.mtype,msg.mtext);
    *pairs_to_test =atoi(msg.mtext);

}

void receive_pairs_from_grader(int worker_id, int pairs_to_test,int msqid){
    for(int i =0;i<pairs_to_test;i++){
        if(i==1){
            printf("inside the worker next round!!!!!!!\n");
        }
        msgbuf_t msg;
        if (msgrcv(msqid, &msg, sizeof(msg.mtext),worker_id,0 ) == -1) {
            perror("msgrcv failed");
            exit(1);
        }
        // printf("in receive_pairs_from_grader: id: %ld %s pairs to test %d and index %d and work_id %d\n",
        //        msg.mtype, msg.mtext, pairs_to_test, i, worker_id);
        int result = sscanf(msg.mtext, "%s %d", pairs[i].executable_path, &pairs[i].parameter);
        if (result != 2) {
            fprintf(stderr, "Error parsing message: %s\n", msg.mtext);
            // Handle the error or continue processing based on your application's logic
        }
        // printf("in receive_pairs_from_grader: id: %ld %s and %d pairs to test %d and index %d and work_id %d\n",
        //        msg.mtype, pairs[i].executable_path, pairs[i].parameter, pairs_to_test, i, worker_id);
       
    }

}
void send_ack_to_grader(int msqid){
    printf("inside send_ack_to_grader\n ");
    msgbuf_t msg;
    msg.mtype = BROADCAST_MTYPE;
    sprintf(msg.mtext, "%s", "0"); // 0 stands for ack and 1 stands for sync
    // strncpy(msg.mtext, pairs_per_worker, sizeof(msg.mtext) - 1);
    if (msgsnd(msqid, &msg, sizeof(msg.mtext), 0) == -1) {
        perror("msgsnd failed");
        // handle the error, possibly with a continue or break statement
    }
}

void receive_ack_from_grader(int msqid){
    while(1){
        sleep(1);
        msgbuf_t msg;
        if (msgrcv(msqid, &msg, sizeof(msg.mtext), BROADCAST_MTYPE,0 ) == -1) {
            perror("msgrcv failed");
            exit(1);
        }
        printf("Received ACK message at pairs from grader: %ld %s\n", msg.mtype,msg.mtext);
        if (strcmp(msg.mtext, "1") == 0){
            break;
        }
        else{
            send_ack_to_grader(msqid);//send it again
        }
    }
}

void set_up_pairs(int pairs_to_test){
    pairs = malloc(pairs_to_test*sizeof(pairs_t));
    for(int i=0;i<pairs_to_test;i++){
        pairs[i].executable_path = malloc(sizeof(char)*100);
    }
}


int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <msqid> <worker_id>\n", argv[0]);
        fprintf(stderr, "Input arguments provided: ");
        for (int i = 0; i < argc; i++) {
            fprintf(stderr, "%s ", argv[i]);
        }
        fprintf(stderr, "\n");
        return 1;
    }

    int msqid = atoi(argv[1]);
    worker_id = atoi(argv[2]);
    

    // TODO: Receive initial message from autograder specifying the number of (executable, parameter) 
    // pairs that the worker will test (should just be an integer in the message body). (mtype = worker_id)

    // TODO: Parse message and set up pairs_t array
    int pairs_to_test;
    receive_pairs_int(worker_id,&pairs_to_test,msqid);
    set_up_pairs(pairs_to_test);

    // TODO: Receive (executable, parameter) pairs from autograder and store them in pairs_t array.
    //       Messages will have the format ("%s %d", executable_path, parameter). (mtype = worker_id)
    receive_pairs_from_grader(worker_id,pairs_to_test,msqid);
    // TODO: Send ACK message to mq_autograder after all pairs received (mtype = BROADCAST_MTYPE)
    send_ack_to_grader(msqid);
    // TODO: Wait for SYNACK from autograder to start testing (mtype = BROADCAST_MTYPE).
    //       Be careful to account for the possibility of receiving ACK messages just sent.
    receive_ack_from_grader(msqid);

    // Run the pairs in batches of 8 and send results back to autograder
    for (int i = 0; i < pairs_to_test; i+= PAIRS_BATCH_SIZE) {
        int remaining = pairs_to_test - i;
        curr_batch_size = remaining < PAIRS_BATCH_SIZE ? remaining : PAIRS_BATCH_SIZE;
        pids = malloc(curr_batch_size * sizeof(pid_t));

        for (int j = 0; j < curr_batch_size; j++) {
            // TODO: Execute the student executable
            execute_solution(pairs[i + j].executable_path, pairs[i + j].parameter, j);
        }

        // TODO: Setup timer to determine if child process is stuck
        // start_timer(TIMEOUT_SECS, timeout_handler);  // Implement this function (src/utils.c)
        PrepareTimer(sa,timers[0],TIMEOUT_SECS);
        // TODO: Wait for the batch to finish and check results
        monitor_and_evaluate_solutions(i);

        // TODO: Cancel the timer if all child processes have finished
        if (child_status == NULL) {
            cancel_timer();
        }

        // TODO: Send batch results (intermediate results) back to autograder
        fprintf(stderr, "before send the result.\n");
        send_results(msqid, worker_id, i,curr_batch_size);

        free(pids);
    }

    // TODO: Send DONE message to autograder to indicate that the worker has finished testing
    send_done_msg(msqid, worker_id,pairs_to_test);

    // Free the pairs_t array
    for (int i = 0; i < pairs_to_test; i++) {
        free(pairs[i].executable_path);
    }
    free(pairs);

    // free(pids);
}
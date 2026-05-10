#include "headers.h"
#include "shared.h"
#include <string.h>
#include <errno.h>

int remainingtime;
int id;

int req_mq;
int ack_mq;
int sync_sem;

MemRequest requests[MAX_REQUESTS];
int num_requests = 0;

static void load_requests() {
    char infile[32];
    FILE *fp;
    char line[256];

    snprintf(infile, sizeof(infile), "requests_%d.txt", id);
    fp = fopen(infile, "r");
    if (!fp) {
        snprintf(infile, sizeof(infile), "requests%d.txt", id);
        fp = fopen(infile, "r");
        if (!fp) return;
    }

    while (fgets(line, sizeof(line), fp)) {
        int t; char address[64]; char rw;
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') continue;
        if (num_requests >= MAX_REQUESTS) break;
        if (sscanf(line, "%d %63s %c", &t, address, &rw) != 3) continue;

        requests[num_requests].time = t;
        strncpy(requests[num_requests].va_str, address, sizeof(requests[num_requests].va_str) - 1);
        requests[num_requests].va = (int)strtol(address, NULL, (strncmp(address, "0x", 2) == 0 || strncmp(address, "0X", 2) == 0) ? 0 : 2);
        requests[num_requests].is_write = (rw == 'w' || rw == 'W');
        num_requests++;
    }
    for (int i = 1; i < num_requests; ++i) {
        MemRequest key = requests[i];
        int j = i - 1;
        while (j >= 0 && requests[j].time > key.time) {
            requests[j + 1] = requests[j];
            --j;
        }
        requests[j + 1] = key;
    }
    fclose(fp);
}

static void sem_down_proc() {
    struct sembuf op;
    op.sem_num = id;
    op.sem_op = -1;
    op.sem_flg = 0;
    while (semop(sync_sem, &op, 1) == -1) {
        if (errno == EINTR) continue;
        perror("semop down proc");
        exit(1);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) return 1;
    remainingtime = atoi(argv[1]);
    id = atoi(argv[2]);

    load_requests();

    req_mq = msgget(PROC_REQ_MQ_KEY, 0666);
    ack_mq = msgget(PROC_ACK_MQ_KEY, 0666);
    sync_sem = semget(PROC_SYNC_SEM_KEY, MAX_PROCESSES + 1, 0666);

    initClk();

    int consumed = 0;
    int next_req_idx = 0;

    while (remainingtime > 0) {
        sem_down_proc();

        while (next_req_idx < num_requests && requests[next_req_idx].time == consumed) {
            ProcReqMsg req;
            req.mtype = id;
            req.msg_type = MSG_MEM_REQ;
            req.va = requests[next_req_idx].va;
            req.is_write = requests[next_req_idx].is_write;
            strncpy(req.va_str, requests[next_req_idx].va_str, sizeof(req.va_str));

            msgsnd(req_mq, &req, sizeof(ProcReqMsg) - sizeof(long), 0);

            ProcAckMsg ack;
            msgrcv(ack_mq, &ack, sizeof(ProcAckMsg) - sizeof(long), id, 0);

            next_req_idx++;

            if (ack.fault) {
                consumed++;
                remainingtime--;
                goto next_tick;
            }
        }

        ProcReqMsg comp;
        comp.mtype = id;
        comp.msg_type = MSG_COMPUTE;
        msgsnd(req_mq, &comp, sizeof(ProcReqMsg) - sizeof(long), 0);

        consumed++;
        remainingtime--;
    next_tick:
        continue;
    }

    destroyClk(false);
    return 0;
}

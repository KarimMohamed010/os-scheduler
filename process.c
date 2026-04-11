#include "headers.h"

/* Modify this file as needed*/
int remainingtime;

int main(int argc, char *argv[])
{
    initClk();

    remainingtime = atoi(argv[1]);
    int last = getClk();

    while (remainingtime > 0)
    {
        int now = getClk();
        if (now > last)
        {
            remainingtime--;
            last = now;
        }
    }

    destroyClk(false);
    return 0;
}
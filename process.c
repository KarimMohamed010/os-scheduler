#include "headers.h"

int remainingtime;

int main(int agrc, char *argv[])
{
    int last_clk;
    int now;

    if (agrc < 2)
    {
        return 1;
    }

    remainingtime = atoi(argv[1]);
    if (remainingtime < 0)
    {
        remainingtime = 0;
    }

    initClk();
    last_clk = getClk();

    while (remainingtime > 0)
    {
        now = getClk();
        if (now > last_clk)
        {
            /* Only consume one tick on consecutive time steps; larger gaps are paused intervals. */
            if (now == last_clk + 1)
            {
                remainingtime--;
            }
            last_clk = now;
        }
        else
        {
            usleep(1000);
        }
    }

    destroyClk(false);

    return 0;
}

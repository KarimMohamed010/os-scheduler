#include "headers.h"

int remainingtime;
static volatile sig_atomic_t continued = 0;

static void on_sigcont(int signum)
{
    (void)signum;
    continued = 1;
}

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

    if (signal(SIGCONT, on_sigcont) == SIG_ERR)
    {
        destroyClk(false);
        return 1;
    }

    while (remainingtime > 0)
    {
        if (continued)
        {
            /* Ignore paused duration: runtime should only decrease while actually running. */
            last_clk = getClk();
            continued = 0;
        }

        now = getClk();
        if (now > last_clk)
        {
            /* Consume exactly the elapsed clock ticks to stay aligned with scheduler timing. */
            remainingtime -= (now - last_clk);
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

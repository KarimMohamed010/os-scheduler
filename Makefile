build:
	gcc process_generator.c -o process_generator.out
	gcc clk.c -o clk.out
	gcc -c mmu.c -o mmu.out
	gcc scheduler.c mmu.out -o scheduler.out
	gcc master_scheduler.c mmu.out -o master_scheduler.out
	gcc process.c -o process.out
	gcc test_generator.c -o test_generator.out

clean:
	rm -f *.out  processes.txt *.log *.perf *.out *.o

all: clean build

run:
	./process_generator.out

stress-fcfs2: build
	bash ./scripts/fcfs2_stress_test.sh

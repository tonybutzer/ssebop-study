# pipe-runner

- takes workorders from the todo and mv them to pending
- then processes the work and mv the workorder to completed
- the lanuncher parent then moves them from completed to terminated and shuts down the machine


# ref

- https://unix.stackexchange.com/questions/454957/cron-job-to-run-under-conda-virtual-environment

```
source /root/miniconda3/etc/profile.d/conda.sh && \
conda activate ssebop-study && \
python3 <your_application> &
```

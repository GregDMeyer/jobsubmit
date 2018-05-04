
'''
An example script for `submitter.py`

Summary
-------

The variable 'batch' contains a batch script. A bunch of fields denoted by curly braces
will be subsituted into automatically. They are first substituted into by the options in
the dicts in `task_options`, and each of these will be submitted as a separate task. Then,
the rest are substituted by the options in `global_options`. If any of the keys are lists,
an array of jobs will be submitted for each task, with each possible combination of values
from all lists substituted in.

Run

python example.py --dry-run

to see how it works!
'''

from os.path import expanduser,join

# the batch script
# important parts:
#
# - for output and error files, remember to include %a to get output from each array run
# - the options are stored in "{output_dir}/{run_idx}_$SLURM_ARRAY_TASK_ID.opts".

batch = '''#!/bin/bash -l
#SBATCH -t {time}
#SBATCH -J {name}
#SBATCH -o {output_dir}/{name}_%a.out
#SBATCH -e {output_dir}/{name}_%a.err
#SBATCH -p {partition}
#SBATCH -n {cores}
#SBATCH -N {nodes}
#SBATCH --mem-per-cpu {mem}

source $HOME/dynamite_current/.venv/bin/activate
export OMP_NUM_THREADS=1
srun --mpi=pmi2 -n {cores} python $HOME/SYK/run_SYK.py $(cat {output_dir}/{task_idx}_${SLURM_ARRAY_TASK_ID}.opts)
'''

program_args = '-L {size} -B {beta} -s --start {start} --stop {stop} --points {points} ' +\
               '--outfile {output_dir}/{name}_{task_idx} --H_idx {H_idx}'

global_options = {
    # batch options
    'name'       : 'my_example_job',
    'output_dir' : expanduser('~/output'),
    'partition'  : 'shared',
    'cores'      : 32,
    'nodes'      : 1,
    'mem'        : '2G',

    # program options
    'H_idx'      : [0, 1],
    'beta'       : [10**x for x in [1, 2]],
    'start'      : 3.0,
    'stop'       : 5.0,
    'points'     : 5,
}

# set a custom directory in output_dir for this particular run
global_options['output_dir'] = join(global_options['output_dir'], global_options['name'])

# each of these will be submitted as a separate job array
task_options = [
    {
        'size':23,
        'time':'9:00:00',
    },
    {
        'size':22,
        'time':'4:30:00',
    },
    {
        'size':21,
        'time':'2:30:00',
    },
]

if __name__ == '__main__':
    from submitter import submit
    submit(batch, program_args, global_options, task_options)

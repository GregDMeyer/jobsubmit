
from copy import deepcopy
from time import sleep
from itertools import product
import argparse as ap
from subprocess import check_output
from os import path, mkdir

def submit(batch, program_args, global_options, task_options, dry_run = None):
    '''
    Submit a job to the batch system.

    Parameters
    ----------

    batch : str
        A string containing the framework of the batch script, with substitutable fields in
        curly braces.

        Fields that should be defined:
         - "name", which is appended to with the task number

        Extra fields which can be specified are:
         - "task_idx", incremented for each task submitted

    program_args : string
        A string representing the command line options, with substitutable fields in braces

    global_options : dict
        A dict containing keys corresponding to the fields in curly braces, with the default
        options for all tasks

    task_options : dict
        A dict with keys corresponding to the parameters in curly braces, to be substituted
        for each batch task submitted

    Returns
    -------
    '''

    if dry_run is None:
        parser = ap.ArgumentParser()
        parser.add_argument('--dry-run',help='output batch script but do not submit',
                            action="store_true")
        args = parser.parse_args()
        dry_run = args.dry_run

    for k in global_options:
        if '{'+str(k)+'}' not in batch + program_args:
            print('Warning: key %s from global_options not used.' % k)

    for n,d in enumerate(task_options):

        # be nice to the scheduler
        if n > 0 and not dry_run:
            sleep(1)

        tmp_vals = deepcopy(global_options)
        tmp_vals.update(d)
        tmp_vals.update({'task_idx':n})

        # append job index to the job name
        tmp_vals['name'] = '{:d}_'.format(n) + tmp_vals['name']

        # also make sure we don't overwrite the actual curly braces we want
        # (this is kind of ridiculous)
        tmp_vals['SLURM_ARRAY_TASK_ID'] = '{SLURM_ARRAY_TASK_ID}'

        batch_out = batch.format(**tmp_vals).format(**tmp_vals)

        if args.dry_run:
            print('\n\n##############################################')
            print('TASK %d\n' % n)
            print('================ BATCH SCRIPT ================\n')
            print(batch_out)
        else:
            # create output directory if it doesn't exist
            out_path = tmp_vals['output_dir'].rstrip('/')
            to_make = []
            while out_path and not path.exists(out_path):
                to_make.append(out_path)
                out_path = path.split(out_path)[0]
            for p in to_make[::-1]:
                mkdir(p)

            with open(path.join(tmp_vals['output_dir'], tmp_vals['name'])+'.batch','x') as f:
                f.write(batch_out)

        if args.dry_run:
            print('================ PROGRAM ARGUMENTS ================\n')

        expanded = expand_vals([tmp_vals])
        for nv,v in enumerate(expanded):

            v.update({'array_idx':nv})
            o = program_args.format(**v)

            if args.dry_run:
                print(nv, ':\n', o, sep='', end = '\n\n')
            else:
                with open(path.join(tmp_vals['output_dir'],str(n)+'_'+str(nv)+'.opts'),'w') as f:
                    f.write(o)

        nv = len(expanded)

        command = ['sbatch','--array=0-%d' % (nv-1)]
        if args.dry_run:
            print('\n================ SUBMIT COMMAND ================\n')
            print('Command:',' '.join(command))
        else:
            print(check_output(command, input=batch_out, universal_newlines=True), end='')
            print('(%d tasks)' % nv)

def expand_vals(vals):
    '''
    Take dictionary or a list of dictionaries, for which some values may be lists, and expand
    them as a list of dictionaries for which the values are taken from the "tensor product" of
    the lists.
    '''

    if isinstance(vals,dict):
        vals = [vals]

    out_dicts = []
    for d in vals:
        l = []
        for k,v in d.items():
            if isinstance(v,str):
                v = [v]

            try:
                v = iter(v)
            except TypeError:
                v = [v]

            l.append([(k,str(x)) for x in v])

        p = product(*l)
        for val_set in p:
            out_dicts.append(dict(val_set))

    return out_dicts

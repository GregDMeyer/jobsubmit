
import argparse as ap
from subprocess import check_output
from os import path,mkdir
from expand_vals import expand_vals
from copy import deepcopy
from time import sleep
from params import vals_default, vals_mod, batch, opts

parser = ap.ArgumentParser()
parser.add_argument('--dry-run',help='output batch script but do not submit',
                    action="store_true")
args = parser.parse_args()

# make sure we won't overwrite
if path.exists(vals_default['output_dir']) and not args.dry_run:
    if not input('output directory exists, overwrite? ') in ['y','Y']:
        exit()
        
for n,d in enumerate(vals_mod):

    # be nice to the scheduler
    if n > 0:
        sleep(1)
        
    tmp_vals = deepcopy(vals_default)
    tmp_vals.update(d)
    tmp_vals.update({'run_idx':n})
    
    # append job index to the job name
    tmp_vals['name'] = '{:d}_'.format(n) + tmp_vals['name']
    
    batch_out = batch.format(**tmp_vals).format(**tmp_vals)

    if args.dry_run:
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

        with open(path.join(tmp_vals['output_dir'],tmp_vals['name'])+'.batch','w') as f:
            f.write(batch_out)
            
    for nv,v in enumerate(expand_vals([tmp_vals])):

        v.update({'task_idx':nv})
        o = opts.format(**v)
    
        if args.dry_run:
            print(nv,':\n',o,sep='')
        else:
            with open(path.join(tmp_vals['output_dir'],str(n)+'_'+str(nv)+'.opts'),'w') as f:
                f.write(o)

    command = ['sbatch','--array=0-%d' % nv]
    if args.dry_run:
        print('Command:',' '.join(command))
    else:
        print(check_output(command,input=batch_out,universal_newlines=True),end='')
        print('(%d tasks)' % (nv+1))


from itertools import product

def expand_vals(vals):

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

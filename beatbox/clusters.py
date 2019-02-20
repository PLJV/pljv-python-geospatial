#!/usr/bin/env python3

def min_max_center(df=None): return df.apply(lambda x: ( x - x.min() / x.max() - x.min() ) )
def mean_variance_center(df=None): return df.apply(lambda x: ( x - x.mean() / x.std() ) )

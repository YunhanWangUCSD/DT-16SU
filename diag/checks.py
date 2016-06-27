''' Utility functions for checking Hadoop configuration '''

def check(dict):
    '''Top-level function for checking Hadoop configuration'''
    chkMem(dict)
    chkCore(dict)

def chkMem(dict):
    mem_max_name = 'yarn.scheduler.maximum-allocation-mb'
    mem_min_name = 'yarn.scheduler.minimum-allocation-mb'
    if not mem_max_name in dict:
        raise ValueError('Error: {0} not found'.format(mem_max_name))
    if not mem_min_name in dict:
        raise ValueError('Error: {0} not found'.format(mem_min_name))

    mem_max = int(dict[mem_max_name])
    mem_min = int(dict[mem_min_name])

    if mem_max < mem_min in dict:
        raise ValueError('Error: {0} = {1} < {2} = {3}'.format(
            mem_max_name, mem_max, mem_min, mem_min_name))
    if mem_max < 2048:
        raise ValueError('Error: {0} = {1} too small'.format(mem_max, mem_max_name))
    if mem_min < 128:
        raise ValueError('Error: {0} = {1} too small'.format(mem_min, mem_min_name))

def chkCore(dict):
    vcores_max_name = 'yarn.scheduler.maximum-allocation-vcores'
    vcores_min_name = 'yarn.scheduler.minimum-allocation-vcores'
    vcores_name     = 'yarn.nodemanager.resource.cpu-vcores'

    if not vcores_max_name in dict:
        raise ValueError('Error: {0} not found'.format(vcores_max_name))
    if not vcores_min_name in dict:
        raise ValueError('Error: {0} not found'.format(vcores_min_name))
    if not vcores_name in dict:
        raise ValueError('Error: {0} not found'.format(vcores_name))

    vcores_max = int(dict[vcores_max_name])
    vcores_min = int(dict[vcores_min_name])
    vcores     = int(dict[vcores_name])

    if vcores_max < vcores_min:
        raise ValueError('Error: {0} = {1} < {2} = {3}'.format(
            vcores_max_name, vcores_max, vcores_min, vcores_min_name))
    if vcores_max < 4:
        raise ValueError('Error: {0} = {1} too small'.format(vcores_max, vcores_max_name))
    if vcores < 4:
        raise ValueError('Error: {0} = {1}  too small'.format(vcores, vcores_name))
    if vcores_min > vcores:
        raise ValueError('Error: {0} = {1} > {2} = {3} '.format(
	    vcores_min_name, vcores_min, vcores, vcores_name))

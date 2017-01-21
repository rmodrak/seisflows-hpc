
import os
import re
import sys

from os.path import abspath, basename, join
from seisflows.tools import unix
from seisflows.tools.code import call, findpath, saveobj
from seisflows.config import ParameterError, custom_import

PAR = sys.modules['seisflows_parameters']
PATH = sys.modules['seisflows_paths']


class slurm_sm_dsh(custom_import('system', 'base')):
    """ An interface through which to submit workflows, run tasks in serial or 
      parallel, and perform other system functions.

      By hiding environment details behind a python interface layer, these 
      classes provide a consistent command set across different computing
      environments.

      Intermediate files are written to a global scratch path PATH.SCRATCH,
      which must be accessible to all compute nodes.

      Optionally, users can provide a local scratch path PATH.LOCAL if each
      compute node has its own local filesystem.

      For important additional information, please see 
      http://seisflows.readthedocs.org/en/latest/manual/manual.html#system-configuration
    """


    def check(self):
        """ Checks parameters and paths
        """

        # check parameters
        if 'TITLE' not in PAR:
            setattr(PAR, 'TITLE', basename(abspath('.')))

        if 'WALLTIME' not in PAR:
            setattr(PAR, 'WALLTIME', 30.)

        if 'VERBOSE' not in PAR:
            setattr(PAR, 'VERBOSE', 1)

        if 'NPROC' not in PAR:
            raise ParameterError(PAR, 'NPROC')

        if 'NTASK' not in PAR:
            raise ParameterError(PAR, 'NTASK')

        if 'SLURMARGS' not in PAR:
            setattr(PAR, 'SLURMARGS', '')

        # check paths
        if 'SCRATCH' not in PATH:
            setattr(PATH, 'SCRATCH', join(abspath('.'), 'scratch'))

        if 'LOCAL' not in PATH:
            setattr(PATH, 'LOCAL', None)

        if 'SUBMIT' not in PATH:
            setattr(PATH, 'SUBMIT', abspath('.'))

        if 'OUTPUT' not in PATH:
            setattr(PATH, 'OUTPUT', join(PATH.SUBMIT, 'output'))


    def submit(self, workflow):
        """ Submits workflow
        """
        unix.mkdir(PATH.OUTPUT)
        unix.cd(PATH.OUTPUT)

        self.checkpoint()

        # submit workflow
        call('sbatch '
                + '%s ' %  PAR.SLURMARGS
                + '--job-name=%s '%PAR.TITLE
                + '--output=%s '%(PATH.SUBMIT +'/'+ 'output.log')
                + '--cpus-per-task=%d '%PAR.NPROC
                + '--ntasks=%d '%PAR.NTASK
                + '--time=%d '%PAR.WALLTIME
                + findpath('seisflows.system') +'/'+ 'wrappers/submit '
                + PATH.OUTPUT)


    def run(self, classname, funcname, hosts='all', **kwargs):
        """  Runs tasks in serial or parallel on specified hosts
        """
        self.checkpoint()
        self.save_kwargs(classname, funcname, kwargs)

        if hosts == 'all':
            # run on all available nodes
            call(findpath('seisflows.system')  +'/'+'wrappers/dsh '
                    + PATH.OUTPUT + ' '
                    + classname + ' '
                    + funcname + ' '
                    + findpath('seisflows.system')  +'/'+'wrappers/run '
                    + ','.join(self.generate_nodelist()))

        elif hosts == 'head':
            # run on head node
            call('ssh ' + self.generate_nodelist()[0] + ' '
                    + '"'
                    + 'export SEISFLOWS_TASK_ID=0; '
                    + join(findpath('seisflows.system'), 'wrappers/run ')
                    + PATH.OUTPUT + ' '
                    + classname + ' '
                    + funcname 
                    +'"')

        else:
            raise(KeyError('Hosts parameter not set/recognized.'))


    def generate_nodelist(self):
        tasks_per_node = []
        for pattern in os.getenv('SLURM_TASKS_PER_NODE').split(','):
            match = re.search('([0-9]+)\(x([0-9]+)\)', pattern)
            if match:
                i,j = match.groups()
                tasks_per_node += [int(i)]*int(j)
            else:
                tasks_per_node += [int(pattern)]

        with open('job_nodelist', 'w') as f:
            call('scontrol show hostname $SLURM_JOB_NODEFILE', stdout=f)

        with open('job_nodelist', 'r') as f:
            nodes = f.read().splitlines() 

        nodelist = []
        for i,j in zip(nodes, tasks_per_node):
            nodelist += [i]*j
        return nodelist
        

    def getnode(self):
        """ Gets number of running task
        """
        return int(os.getenv('SEISFLOWS_TASK_ID'))


    def mpiexec(self):
        """ Specifies MPI exectuable; used to invoke solver
        """
        return ''
        #return 'mpirun -np %d '%PAR.NPROC


    def save_kwargs(self, classname, funcname, kwargs):
        kwargspath = join(PATH.OUTPUT, 'kwargs')
        kwargsfile = join(kwargspath, classname+'_'+funcname+'.p')
        unix.mkdir(kwargspath)
        saveobj(kwargsfile, kwargs)


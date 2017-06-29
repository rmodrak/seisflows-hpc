
import sys

from getpass import getuser
from os.path import abspath, exists
from uuid import uuid4
from seisflows.tools import unix
from seisflows.config import ParameterError, custom_import

PAR = sys.modules['seisflows_parameters']
PATH = sys.modules['seisflows_paths']


class tigergpu_lg(custom_import('system', 'slurm_lg'))
    """ Specially designed system interface for tigergpu.princeton.edu

      See parent class for more information.
    """

    def check(self):
        """ Checks parameters and paths
        """
        hostname = unix.hostname()
        if hostname != 'tigergpu':
            print msg.SystemWarning % ('tigergpu', hostname)

        # where job was submitted
        if 'WORKDIR' not in PATH:
            setattr(PATH, 'WORKDIR', abspath('.'))

        # where temporary files are written
        if 'SCRATCH' not in PATH:
            setattr(PATH, 'SCRATCH', PATH.WORKDIR+'/'+'scratch')

        super(tigergpu_lg, self).check()

        assert PAR.NGPU <= 4
        assert PAR.NPROC <= 28


    def submit(self, *args, **kwargs):
        """ Submits job
        """
        if not exists(PATH.SCRATCH):
            path = '/scratch/gpfs'+'/'+getuser()+'/'+'seisflows'+'/'+str(uuid4())
            unix.mkdir(path)
            unix.ln(path, PATH.SCRATCH)

        call('sbatch '
                + '%s ' % PAR.SLURMARGS
                + '--job-name=%s ' % PAR.TITLE
                + '--output %s ' % (PATH.WORKDIR+'/'+'output.log')
                + '--ntasks-per-node=%d ' % 28
                + '--gres=gpu:%d' % 4
                + '--nodes=%d ' % 1
                + '--time=%d ' % PAR.WALLTIME
                + findpath('seisflows.system') +'/'+ 'wrappers/submit '
                + PATH.OUTPUT)


    def job_array_cmd(self, classname, method, hosts):
        return ('sbatch '
                + '%s ' % PAR.SLURMARGS
                + '--job-name=%s ' % PAR.TITLE
                + '--nodes=1 '
                + '--ntasks-per-node=%s ' % PAR.NPROC
                + '--gres=gpu:%d' % NGPU
                + '--ntasks=%d ' % PAR.NPROC
                + '--time=%d ' % PAR.TASKTIME
                + self.job_array_args(hosts)
                + findpath('seisflows.system') +'/'+ 'wrappers/run '
                + PATH.OUTPUT + ' '
                + classname + ' '
                + method + ' '
                + PAR.ENVIRONS)


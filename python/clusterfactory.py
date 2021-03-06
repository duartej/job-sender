#!/usr/bin/env python
""":mod:`clusterfactory` -- clusterspec abstract and concrete classes
======================================================================

.. module:: clusterfactory
   :platform: Unix
   :synopsis: Module which contains the clusterspec abstract class
              and its concrete cluster classes.
              The clusterspec classes interact with the cluster in order
              to send, check, retrieve, abort, ... jobs. The 'clusterspec'
              base class contains those methods which are generic in order
              to deal with any kind of cluster, and defines the virtual
              methods which have to be implemented in each class defining
              a concrete cluster. In this way, the clusterspec class can be 
              used as interface to any client which wants to interact with
              a cluster.
              This module can be use to incorporate the concrete clusterspec
              classes.
.. moduleauthor:: Jordi Duarte-Campderros <jorge.duarte.campderros@cern.ch>
"""

from abc import ABCMeta
from abc import abstractmethod


def get_compact_list(tasklist):
    """ Return a string-like list of all the components of the list
    compactified by joining the edges of a list of consecutives 
    numbers, and printed between a dash (-). For non-consecutive 
    numbers, a comma (,) separate the two closest numbers.
    Example
    -------
    Given a list 
        [1,2,3,4,5,45,48,51,52,53,60], 
    this function will return: 
        '1-5,45,48,51-53,60'

    Parameters
    ----------
    tasklist: list(int)

    Return
    ------
    str: the compactified version of the tasklist
    """
    # Obtaining a list of paired values. The edge are defining intervals
    # of numbers with the same state, note that all the operations are
    # done with the tuples (id,status)
    idinit = sorted(tasklist)[0]
    currentinit = idinit
    currentlast = currentinit
    compactlist = []
    for id in sorted(tasklist)[1:]:
        if id-1 != currentlast:
            compactlist.append( (currentinit,currentlast) )
            currentinit = id
        currentlast = id
    # last set
    compactlist.append( (currentinit,currentlast) )
    premessage = ""
    for (idi,idf) in compactlist:
        if idi == idf:
            premessage+= "{0},".format(idi)
        else:
            premessage+= "{0}-{1},".format(idi,idf)
    message = premessage[:-1]
        
    return message


class clusterspec(object):
    """Abstract class to deal with the cluster interaction. The
    commands to send and monotoring jobs to a batch systems are
    encapsulated here. The concrete classes are defined depending
    the batch manager system and therefore, the specific commands
    to be used, the specific environment variables, ...
    """
    __metaclass__ = ABCMeta
    
    def __init__(self,**kw):#joblist,**kw):
        """Abstract class to deal with the cluster interaction. The
        commands to send and monotoring jobs to a batch systems are
        encapsulated here. The concrete classes are defined depending
        the batch manager system and therefore, the specific commands
        to be used, the specific environment variables, ...

        Attributes
        ----------
        simulate: bool
            whether is a simulation or not
        array_variable: str (NOT IMPLEMENTED, VIRTUAL ATTRIBUTE)
            the name of the environment variable used by the batching
            system to identify an array job
        logout_file: str
            the name of the log output file
        logerr_file: str
            the name of the log error file
        sendcom: str (NOT IMPLEMENTED, VA)
            the name of the command from the batching system to send
            jobs
        extraopt: list(str)
            the options which populate the `sendcom` command
        arrayopt: [str,str]  (NOT IMPLEMENTED, VA)
            the option to deal with the array jobs in the batch system
        arrayformat: (NOT IMPLEMENTED, VA)
            the format of the task jobs (the argument of the array job
            option in the cluster batch system sender command)
        statecom: str (NOT IMPLEMENTED, VA)
            the name of the command to monitor the jobs
        killcom: str (NOT IMPLEMENTED, VA)
            the name of the command to kill jobs
        ID: int  [TO BE DEPRECATED, ACTUALLY NOT NEEDED]
            the identification number of the job in the batch system
        
        Virtual methods
        ---------------
        simulatedresponse: debugging function
        getjobidfromcommand: extract the job identifier 
        getstatefromcommandline: extract the job state
        failed
        done
        """
        # If true, do not interact with the cluster, just for debugging
        self.simulate = False
        if kw.has_key('simulate'):
            self.simulate=kw['simulate']
        # Name of the standard outputfile
        self.logout_file = "STDOUT"
        # Actual command to send jobs
        self.sendcom     = None
        # Extra parameters/option to the command 
        self.extraopt    = [ '-o', self.logout_file, '-e', 'STDERR']
        # Actual command to obtain the state of a job
        self.statecom    = None
        # Actual command to kill a job
        self.killcom     = None
        # List of jobdescription instances
        #self.joblist     = joblist
        # The suffix for the cluster job
        self.script_suffix = None

    def submit(self,jobdsc):
        """Send a job to the cluster
         
        Parameters
        ----------
        jobdsc: jobSender.jobsender.jobdescription
        """
        from subprocess import Popen,PIPE
        import os
        cwd = os.getcwd()
        # Going to directory of sending
        os.chdir(jobdsc.path)
        # Building the command to send to the shell:
        command = [ self.sendcom ]
        for i in self.extraopt:
            command.append(i)
        command.append(jobdsc.script+'.'+self.script_suffix)
        # Extra function for the creation of cluster scripts
        self.create_script_if_needed(jobdsc.script)

        # Send the command
        if self.simulate:
            p = self.simulatedresponse('submit')
        else:
            p = Popen(command,stdout=PIPE,stderr=PIPE).communicate()

        if p[1] != "":
            message = "ERROR from {0}:\n".format(self.sendcom)
            message += p[1]+"\n"
            os.chdir(cwd)
            print "\033[1;31mERROR SENDING JOB TO CLUSTER\033[1;m {0}".format(message)
            self.ID = None
            jobdsc.ID = self.ID
            jobdsc.status = 'fail'
            os.chdir(cwd)
            return
        ## The job-id is released in the message:
        self.ID = self.getjobidfromcommand(p[0])
        jobdsc.ID = self.ID
        print "INFO:"+str(jobdsc.script)+'_'+str(jobdsc.index)+\
                " submitted with cluster ID:"+str(self.ID)
        # Updating the state and status of the job
        jobdsc.state  = 'submitted'
        jobdsc.status = 'ok'
        # Coming back to the original folder
        os.chdir(cwd)
    
    def getnextstate(self,jobdsc,checkfinishedjob):
        """Check the state and status of the job. The life of a job 
        follows the state workflow
            None -> configured -> submitted -> running -> finished
        Per each possible state (and status), just a few commands
        can be used.
        
        Parameters
        ----------
        jobdsc: jobsender.jobdescriptor
        checkfinishedjob: workenvfactory.workenv.checkfinishedjob 
            the function to check if the job has
        """
        if not jobdsc.state:
            print "Job not configured yet, you should call the"\
                    " jobspec.preparejobs method"            
        elif jobdsc.state == 'submitted' or jobdsc.state == 'running':
            jobdsc.state,jobdsc.status=self.checkstate(jobdsc)
            if jobdsc.state == 'finished':
                if self.simulate:
                    self.status = self.simulatedresponse('finishing')
                else:
                    jobdsc.status = checkfinishedjob(jobdsc,self.logout_file)
        #elif (jobdsc.state == 'finished' and jobdsc.status = 'fail') \
        #        or jobdsc.state == 'aborted':

    @abstractmethod
    def simulatedresponse(self,action):
        """..method:: simulatedresponse() -> clusterresponse
        DO NOT USE this function, just for debugging proporses
        method used to simulate the cluster response when an
        action command is sent to the cluster, in order to 
        proper progate the subsequent code.
        """
        raise NotImplementedError("Class %s doesn't implement "\
                "simulatedresponse()" % (self.__class__.__name__))
    
    @abstractmethod
    def getjobidfromcommand(self,p):
        """..method:: getjobidfromcommand()
        
        function to obtain the job-ID from the cluster command
        when it is sended (using sendcom)
        """
        raise NotImplementedError("Class %s doesn't implement "\
                "getjobidfromcommand()" % (self.__class__.__name__))
    
    
    def checkstate(self,jobdsc):
        """..method:: checkstate()
        
        function to check the status of a job (running/finalized/
        aborted-failed,...). 
        """
        from subprocess import Popen,PIPE
        import os
        if jobdsc.state == 'submitted' or jobdsc.state == 'running':
            command = [ self.statecom, "{0}".format(jobdsc.ID) ]
            if self.simulate:
                p = self.simulatedresponse('checking')
            else:
                p = Popen(command,stdout=PIPE,stderr=PIPE).communicate()
            return self.getstatefromcommandline(p)
        else:
            return jobdsc.state,jobdsc.status

    @abstractmethod
    def getstatefromcommandline(self,p):
        """..method:: getstatefromcommandline() -> status
        
        function to obtain the status of a job. Cluster dependent
        """
        raise NotImplementedError("Class %s doesn't implement "\
                 "getstatefromcommandline()" % (self.__class__.__name__))
    
    def kill(self,jobdsc):
        """..method:: kill()
        method to kill running-state jobs.
        """
        from subprocess import Popen,PIPE
        import os
        if jobdsc.state == 'running' or jobdsc.state == 'submitted':
            command = [ self.killcom, str(jobdsc.ID) ]
            if self.simulate:
                p = self.simulatedresponse('killing')
            else:
                p = Popen(command,stdout=PIPE,stderr=PIPE).communicate()
            jobdsc.state  = 'configured'
            jobdsc.status = 'ok'
        else:
            print "WARNING::JOB [%s] not in running or submitted state,"\
                    " kill has no sense" % jobdsc.index

    @abstractmethod
    def create_script_if_needed(self,filename):
        """..method:: create_script_if_neeed() 
        Create cluster specific files (for HTcondor actually)
        """
        raise NotImplementedError("Class %s doesn't implement "\
                "create_scrip_if_needed(filename)" % (self.__class__.__name__))

    @abstractmethod
    def failed(self):
        """..method:: failed()
        steering the actions to proceed when a job has failed.
        Depend on the type of cluster
        """
        raise NotImplementedError("Class %s doesn't implement "\
                 "failed()" % (self.__class__.__name__))

    @abstractmethod
    def done(self):
        """..method:: done()
        steering the actions to be done when the job has been complete
        and done. Depend on the type of the cluster
        """
        raise NotImplementedError("Class %s doesn't implement "\
                 "done()" % (self.__class__.__name__))


# --- Concrete class for the CERN cluster (using the lxplus UI)
class cerncluster(clusterspec):
    """..class:: cerncluster
    Concrete implementation of the clusterspec class dealing with
    the cluster at cern (usign lxplus as UI)

    Notes
    -----
    There is an API of the HTCondor  (import htcondor) which could
    be worth it to introduce..
    """
    def __init__(self,**kw):#joblist=None,**kw):
        """..class:: cerncluster 
        Concrete implementation of the clusterspec class dealing with
        the cluster at cern (usign lxplus as UI)
        """
        super(cerncluster,self).__init__(**kw)#joblist,**kw)
        self.sendcom   = 'condor_submit'
        self.statecom  = 'condor_q -nobatch'
        self.killcom   = 'bkill'
        self.script_suffix = 'sub'
        if kw.has_key('queue') and kw['queue']:
            queue = kw['queue']
            # FIXME: Check that is a condor queue
            # max. duration: 20 min., 1h, 2h, 8h, 1d, 3d, 1w
            available_q = [ 'espresso', 'microcentury', 'longlunch', 'workday',
                    'tomorrow', 'testmatch', 'nextweek' ]
        else:
            queue = 'longlunch'
        self.extraopt  += [ '--queue', queue ]
        ## Need to include the cluster file: need the jobdescription
        
    
    def simulatedresponse(self,action):
        """..method:: simulatedresponse() -> clusterresponse
        DO NOT USE this function, just for debugging proporses
        method used to simulate the cluster response when an
        action command is sent to the cluster, in order to 
        proper progate the subsequent code.
        """
        import random

        if action == 'submit':
            rdnjobnum= int(random.uniform(0,9999999))
            return ("1 job(s) submitted to cluster {0}.".format(rdnjobnum),"")
        elif action == 'checking':
            potentialstate = [ 'submitted', 'running', 'finished', 'aborted',
                    'submitted','running','finished', 'running', 'finished',
                    'running', 'finished', 'finished', 'finished' ]
            # using random to choose which one is currently the job, biasing
            # to finished and trying to keep aborted less probable
            simstate = random.choice(potentialstate)
            rdnjobnum= int(random.uniform(0,9999999))
            if simstate == 'submitted':
                mess = ("\n\n\n{0}.0 duarte 10/1 12:09 0+00:00:00 I  0 0.0 kki.sh {0}.0".format(rdnjobnum),"")
            elif simstate == 'running':
                mess = ("\n\n\n{0}.0 duarte 10/1 12:09 0+00:00:00 R  0 0.0 kki.sh {0}.0".format(rdnjobnum),"")
            elif simstate == 'finished':
                mess = ("\n\n\n{0}.0 duarte 10/1 12:09 0+00:00:00 C  0 0.0 kki.sh {0}.0".format(rdnjobnum),"")
            elif simstate == 'aborted':
                mess = ("\n\n\n{0}.0 duarte 10/1 12:09 0+00:00:00 X  0 0.0 kki.sh {0}.0".format(rdnjobnum),"")
            return mess
        elif action == 'finishing':
            simstatus = [ 'ok','ok','ok','fail','ok','ok','ok']
            return random.choice(simstatus)
        elif action == 'killing':
            return 'configured','ok'
        else:
            raise RuntimeError('Undefined action "%s"' % action)


    def getjobidfromcommand(self,p):
        """..method:: getjobidfromcommand()
        function to obtain the job-ID (clusterID, in HT-Condor notation) 
        from the cluster command when it is sended (using sendcom)
        """
        return int(p.split('cluster ')[-1].split('.')[0])
    
    def getstatefromcommandline(self,p):
        """..method:: getstatefromcommandline() -> state,status
        function to parse the state of a job
        
        Parameters
        ----------
        p: (str,str)
            tuple corresponding to the return value of the 
            subprocess.Popen.communicate, i.e. (stdoutdata, stderrdat)

        Returns
        -------
        id: (str,str)
            the state and status
        
        Note
        ----
        An output example provided by the condor_q --nobatch command is:

        -- Schedd: xxxxxxx.cern.ch : <XXX.XXX.XXX.XXX:XXXX?... @ 10/01/19 12:09:38
         ID         OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
         3205766.0   duarte         10/1  12:09   0+00:00:00 I  0    0.0 digijobs.sh 3205766.0
        """
        # condor_q output
        # ID, OWNER, SUBMITTED, RUN_TIME, ST, PRI, SIZE, CMD

        isfinished=False
        if p[0].find('0 jobs') != -1: 
            return 'finished','ok'
        else:
            # XXX-- Multiple job task : [3:-1]
            jobinfoline = p[0].split('\n')[3]
            # Third element
            status = jobinfoline.split()[5]
            if status == 'I':
                return 'submitted','ok'
            elif status == 'R':
                return 'running','ok'
            elif status == 'C':
                return 'finished','ok'
            # ??? Removed is aborted?
            elif status == 'X':
                return 'aborted','ok'
            ## elif status == 'H':
            #  HOLD status, waiting for someone to re-schedule the job
            ## elif status == 'X':
            #  rEMOVED status, 
            ## elif status == 'S':
            #  suspended  status, execution temp. suspended
            else:
                message='I have no idea of the state parsed in the cluster'
                message+=' as "%s". Parser should be updated\n' % status
                message+='WARNING: forcing "None" state'
                print message
                return None,'fail'
        #else:
        #    message='No interpretation yet of the message (%s,%s).' % (p[0],p[1])
        #    message+='Cluster message parser needs to be updated'
        #    message+='(cerncluster.getstatefromcommandline method).'
        #    message+='\nWARNING: forcing "aborted" state'
        #    print message
        #    return 'aborted','fail'
    
    # DEPRECATED
    #def setjobstate(self,jobds,command):
    #    """..method:: setjobstate(jobds,action) 
    #    establish the state (and status) of the jobds ('jobdescription' 
    #    instance) associated to this clusterspec instance, depending
    #    of the 'command' being executed
    #    """
    #    if command == 'configuring':
    #        self.joblist[-1].state   = 'configured'
    #        self.joblist[-1].status  = 'ok'
    #        self.joblist[-1].jobspec = self
    #    elif command == 'submitting':
    #        self.joblist[-1].state   = 'submit'
    #        self.joblist[-1].status  = 'ok'
    #    else:
    #        raise RuntimeError('Unrecognized command "%s"' % command)
    
    def failed(self):
        """..method:: failed()
         
        steering the actions to proceed when a job has failed.
        Depend on the type of cluster
        """
        raise NotImplementedError("Class %s doesn't implement "\
                 "failed()" % (self.__class__.__name__))

    def done(self):
        """..method:: done()
        steering the actions to be done when the job has been complete
        and done. Depend on the type of the cluster
        """
        raise NotImplementedError("Class %s doesn't implement "\
                 "done()" % (self.__class__.__name__))

    def create_script_if_needed(self,filename):
        """Create the file to be sent to the cluster
        """
        import os 

        lines = ["executable              = {0}".format(filename+'.sh\n')]
        lines+= ["arguments               = $(ClusterId)$(ProcId)\n"]
        lines+= ["output                  = output/$(ClusterId).$(ProcId).out\n"]
        lines+= ["error                   = output/$(ClusterId).$(ProcId).err\n"]
        lines+= ["log                     = output/$(ClusterId).log\n"]
        lines+= ["queue\n"]
        #lines+= ["queue filename matching (exec/job_*sh)"]
        with open('{0}.{1}'.format(filename,self.script_suffix), 'w') as f:#
            f.writelines(lines)
        # And create the output and log folders (if there are not)
        try: 
            os.mkdir('output')
        except OSError:
            pass
        try: 
            os.mkdir('log')
        except OSError:
            pass
    
clusterspec.register(cerncluster)

class taucluster(clusterspec):
    """Concrete implementation of the clusterspec class dealing with
    the cluster at Israel T2 (usign t302.hep.tau.ac.il and 
    t302.hep.tau.ac.ilas UI)
    """
    def __init__(self,**kw):#joblist=None,**kw):
        """
        Parameters
        ----------
        queue: str, { 'N', 'P', 'S', 'atlas', 'HEP' }
            the name of the queue, see details and requirements of each
            queue in `qstat -Q -f`

        """
        super(taucluster,self).__init__(**kw)#joblist,**kw)
        self.sendcom   = 'qsub'
        self.statecom  = 'qstat'
        self.killcom   = 'qdel'
        self.script_suffix = 'sh'
        if kw.has_key('queue') and kw['queue']:
            queue = kw['queue']
        else:
            queue = 'N'
        self.extraopt += [ '-q', queue , '-V' ]
        # Extra options to accomodate
        if kw.has_key('extra_opts') and kw["extra_opts"]:
            for _at in kw['extra_opts'].split(" "):
                self.extraopt.append(_at)
    
    def simulatedresponse(self,action):
        """ DO NOT USE this function, just for debugging proporses
        method used to simulate the cluster response when an
        action command is sent to the cluster, in order to 
        proper progate the subsequent code.

        Parameters
        ----------
        action: str 
            the action to be simulated {'submit','checking','finishing',
            'killing'} 

        Returns
        -------
        clusterresponse: str
            mimic the cluster response of the simulated job depending
            the state and status randomly choosen

        Raises
        ------
        RunTimeError
            if the action parameter is not valid
        """
        import random

        if action == 'submit':
            i=xrange(0,9)
            z=''
            for j in random.sample(i,len(i)):
                z+=str(j)
            return ("{0}.tau-cream.hep.tau.ac.il".format(z),"")
        elif action == 'checking':
            potentialstate = [ 'submitted', 'running', 'finished', 'aborted',
                    'submitted','running','finished', 'running', 'finished',
                    'running', 'finished', 'finished', 'finished' ]
            # using random to choose which one is currently the job, biasing
            # to finished and trying to keep aborted less probable
            simstate = random.choice(potentialstate)
            if simstate == 'submitted':
                mess = ("Job id <123456789>:\n----\nsim the job status Q","")
            elif simstate == 'running':
                mess = ("Job id <123456789>:\n----\nsim the job status R","")
            elif simstate == 'finished':
                mess = ("Job id <123456789>:\n----\nsim the job status C","")
            elif simstate == 'aborted':
                mess = ("Job id <123456789>:\n----\nsim the job status E","")
            return mess
        elif action == 'finishing':
            simstatus = [ 'ok','ok','ok','fail','ok','ok','ok']
            return random.choice(simstatus)
        elif action == 'killing':
            return 'configured','ok'
        else:
            raise RuntimeError('Undefined action "%s"' % action)


    def getjobidfromcommand(self,p):
        """Obtain the job-ID from the cluster command
        when it is sended (using sendcom)
        
        Parameters
        ----------
        p: (str,str)
            tuple corresponding to the return value of the 
            subprocess.Popen.communicate, i.e. (stdoutdata, stderrdat)

        Returns
        -------
        id: int
            the job id 

        Notes
        -----
        The generic job-id is given by a INT.tau-cream.hep.tau.ac.il,
        being INT an integer followed by the server name

        The new attribute `server_name` is created in this method 
        if does not exist before
        """
        # new attribute
        if not hasattr(self,"server_name"):
            self.server_name = '.'.join(p.split('.')[1:])
        return int(p.split('.')[0])
    
    def getstatefromcommandline(self,p):
        """parse the state of a job
        
        Parameters
        ----------
        p: (str,str)
            tuple corresponding to the return value of the 
            subprocess.Popen.communicate, i.e. (stdoutdata, stderrdat)

        Returns
        -------
        id: (str,str)
            the state and status

        Notes
        -----
        The way is obtained the status is through the qstat JOBID which
        follows the structure
            Job id          Name    User    Time    Use   Status  Queue
            -----------------------------------------------------------
            JOBID_INT       blah     me      blah   bleh    S     bleah
        """
        # qstat output
        # Job id  Name User Time Use Status Queue

        isfinished=False
        if p[0].find('qstat: Unknown Job Id') != -1 or \
                p[1].find('qstat: Unknown Job Id') != -1: 
            return 'finished','ok'
        elif p[0].find('Job id') == 0:
            # -- second line (after header and a line of -)
            jobinfoline = p[0].split('\n')[2]
            # fourth element
            status = jobinfoline.split()[4]
            if status == 'Q':
                return 'submitted','ok'
            elif status == 'R':
                return 'running','ok'
            elif status == 'C':
                return 'finished','ok'
            elif status == 'E':
                return 'aborted','ok'
            else:
                kw
                message='I have no idea of the state parsed in the cluster'
                message+=' as "%s". Parser should be updated\n' % status
                message+='WARNING: forcing "None" state'
                print message
                return None,'fail'
        else:
            message='No interpretation yet of the message (%s,%s).' % (p[0],p[1])
            message+=' Cluster message parser needs to be updated'
            message+='(taucluster.getstatefromcommandline method).'
            message+='\nWARNING: forcing "aborted" state'
            print message
            return 'aborted','fail'
    
    def failed(self):
        """..method:: failed()
         
        steering the actions to proceed when a job has failed.
        Depend on the type of cluster
        """
        raise NotImplementedError("Class %s doesn't implement "\
                 "failed()" % (self.__class__.__name__))

    def done(self):
        """..method:: done()
        steering the actions to be done when the job has been complete
        and done. Depend on the type of the cluster
        """
        raise NotImplementedError("Class %s doesn't implement "\
                 "done()" % (self.__class__.__name__))

    def create_script_if_needed(self,filename):
        """Do not need to do anything
        """
        return
    
clusterspec.register(taucluster)


def cluster_builder(**kw):
    """builder checking the running machine and instantiate
    the proper clusterspec class

    See Also
    --------
    cerncluster: the concrete class for the CERN LXBATCH system
    taucluster : the concrete class for the T2 @ TAU 

    Raises
    ------
    NotImplementedError
        whenever this method is called in an unknown machine
    """
    import socket
    machine = socket.gethostname()

    # build the proper cluster depending where we are
    # cern and T2 at tau 
    if machine.find('lxplus') == 0:
        # Add the suffix for the sender script
        kw['script_suffix'] = 'sub'
        return cerncluster(**kw)
    elif machine.find('tau.ac.il') != -1:
        kw['script_suffix'] = 'sh'
        return taucluster(**kw)
    else:
        raise NotImplementedError("missing cluster for UI: '{0}'".format(machine))             


""" FSM: refered to Fantasm, delete GAE taskqueue

Copyright 2010 VendAsta Technologies Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.



The FSM implementation is inspired by the paper:

[1] J. van Gurp, J. Bosch, "On the Implementation of Finite State Machines", in Proceedings of the 3rd Annual IASTED
    International Conference Software Engineering and Applications,IASTED/Acta Press, Anaheim, CA, pp. 172-178, 1999.
    (www.jillesvangurp.com/static/fsm-sea99.pdf)
"""

import datetime
import random
import copy
import time
import sys

if sys.version_info < (2, 7):
    import simplejson as json
else:
    import json

import pickle

from fantasm import constants, config
from fantasm.log import Logger
from fantasm.state import State
from fantasm.transition import Transition
from fantasm.exceptions import UnknownEventError, UnknownStateError, UnknownMachineError, TRANSIENT_ERRORS, \
                               HaltMachineError
from fantasm.models import _FantasmFanIn, _FantasmInstance
from fantasm import models
from fantasm.utils import knuthHash
from fantasm.lock import ReadWriteLock, RunOnceSemaphore

from hxfsm import FSMContext

class FSM(object):
    """ An FSMContext creation factory. This is primarily responsible for translating machine
    configuration information (config.currentConfiguration()) into singleton States and Transitions as per [1]
    """

    PSEUDO_INIT     = 'pseudo-init'
    PSEUDO_FINAL    = 'pseudo-final'

    _CURRENT_CONFIG = None
    _MACHINES       = None
    _PSEUDO_INITS   = None
    _PSEUDO_FINALS  = None

    def __init__(self, currentConfig=None):
        """ Constructor which either initializes the module/class-level cache, or simply uses it

        @param currentConfig: a config._Configuration instance (dependency injection). if None,
            then the factory uses config.currentConfiguration()
        """
        # Fantasm use YAML to describe FSM, hxfsm use json to describe FSM 
        currentConfig = currentConfig or config.currentConfiguration()

        # if the FSM is not using the currentConfig (.yaml was edited etc.)
        # no need to store current config, it's ugly
        if not (FSM._CURRENT_CONFIG is currentConfig):
            self._init(currentConfig=currentConfig)
            FSM._CURRENT_CONFIG = self.config
            FSM._MACHINES = self.machines
            FSM._PSEUDO_INITS = self.pseudoInits
            FSM._PSEUDO_FINALS = self.pseudoFinals

        # otherwise simply use the cached currentConfig etc.
        # 
        else:
            self.config = FSM._CURRENT_CONFIG
            self.machines = FSM._MACHINES
            self.pseudoInits = FSM._PSEUDO_INITS
            self.pseudoFinals = FSM._PSEUDO_FINALS

    def __init__(self, FSMDict):
        """ Constructor which either initializes the module/class-level cache, or simply uses it

        @param currentConfig: a config._Configuration instance (dependency injection). if None,
            then the factory uses config.currentConfiguration()
        """
        # Fantasm use YAML to describe FSM, hxfsm use json to describe FSM 
        import logging
        if FSM.machines[FSMDict['name']]:
            logging.info("FSM name " + FSMDict['name'] + "already used!")
            return


        # if the FSM is not using the currentConfig (.yaml was edited etc.)
        # no need to store current config, it's ugly
        if not (FSM._CURRENT_CONFIG is currentConfig):
            self._init(currentConfig=currentConfig)
            FSM._CURRENT_CONFIG = self.config
            FSM._MACHINES = self.machines
            FSM._PSEUDO_INITS = self.pseudoInits
            FSM._PSEUDO_FINALS = self.pseudoFinals

        # otherwise simply use the cached currentConfig etc.
        # 
        else:
            self.config = FSM._CURRENT_CONFIG
            self.machines = FSM._MACHINES
            self.pseudoInits = FSM._PSEUDO_INITS
            self.pseudoFinals = FSM._PSEUDO_FINALS
    def _init(self, name, statesList, transitionsList, currentConfig=None):
        """ Constructs a group of singleton States and Transitions from the machineConfig

        @param name             : FSM machine name
        @param statesList       : all states of this FSM
        @param transitionList   : all transition of this FSM
        @param currentConfig: a config._Configuration instance (dependency injection). if None,
            then the factory uses config.currentConfiguration()
        """
        import logging
        logging.info("Initializing FSM machine.")

        self.config = currentConfig or config.currentConfiguration()
        
        self.machines = {}
        self.pseudoInits, self.pseudoFinals = {}, {}
        #for machineConfig in self.config.machines.values():
            # initial one machines one time 
            # machine looks like {'states'      : {'stateName1': stateObj1,
            #                                      'stateName2': stateObj2},
            #                     'transitions' : {'transitionName1': transObj1,
            #                                      'transitionName2': transObj2}}
            FSM.machines[name] = {constants.MACHINE_STATES_ATTRIBUTE: {},
                                  constants.MACHINE_TRANSITIONS_ATTRIBUTE: {}}
            #self.machines[name] = {constants.MACHINE_STATES_ATTRIBUTE: {},
            #                     constants.MACHINE_TRANSITIONS_ATTRIBUTE: {}}
 
            machine = self.machines[machineConfig.name]

            # create a pseudo-init state for each machine that transitions to the initialState
            pseudoInit = State(FSM.PSEUDO_INIT, None, None, None)
            self.pseudoInits[machineConfig.name] = pseudoInit
            self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][FSM.PSEUDO_INIT] = pseudoInit

            # create a pseudo-final state for each machine that transitions from the finalState(s)
            pseudoFinal = State(FSM.PSEUDO_FINAL, None, None, None, isFinalState=True)
            self.pseudoFinals[machineConfig.name] = pseudoFinal
            self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][FSM.PSEUDO_FINAL] = pseudoFinal

            # add states to FSM.machines
            map(lambda state: FSM.machines[name][state.name] = state, statesList)

            # add transition to FSM.machines
            stateDict = FSM.machines[name][constants.MACHINE_STATES_ATTRIBUTE]
            transDict = FSM.machines[name][constants.MACHINE_TRANSITIONS_ATTRIBUTE]
            def _addTranstion(stateDict, transDict, transition):
                transDict[transition.name]      = transition
                stateDict[transition.source].addTransition(transition, transition.event) 
            map(_addTranstion, transitionsList)

            for stateConfig in machineConfig.states.values():
                state = self._getState(machineConfig, stateConfig)

                # add the transition from pseudo-init to initialState
                if state.isInitialState:
                    # transite from PSEUDO_INIT to initial state
                    transition = Transition(FSM.PSEUDO_INIT, state,
                                            retryOptions = self._buildRetryOptions(machineConfig),
                                            queueName=machineConfig.queueName)
                    self.pseudoInits[machineConfig.name].addTransition(transition, FSM.PSEUDO_INIT)

                # add the transition from finalState to pseudo-final
                if state.isFinalState:
                    transition = Transition(FSM.PSEUDO_FINAL, pseudoFinal,
                                            retryOptions = self._buildRetryOptions(machineConfig),
                                            queueName=machineConfig.queueName)
                    state.addTransition(transition, FSM.PSEUDO_FINAL)

                machine[constants.MACHINE_STATES_ATTRIBUTE][stateConfig.name] = state

'''
            for transitionConfig in machineConfig.transitions.values():
                source = machine[constants.MACHINE_STATES_ATTRIBUTE][transitionConfig.fromState.name]
                transition = self._getTransition(machineConfig, transitionConfig)
                machine[constants.MACHINE_TRANSITIONS_ATTRIBUTE][transitionConfig.name] = transition
                event = transitionConfig.event
                source.addTransition(transition, event)
'''
    def load
    def _getState(self, machineConfig, stateConfig):
        """ Returns a State instance based on the machineConfig/stateConfig

        @param machineConfig: a config._MachineConfig instance
        @param stateConfig: a config._StateConfig instance
        @return: a State instance which is a singleton wrt. the FSM instance
        """

        if machineConfig.name in self.machines and \
           stateConfig.name in self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE]:
            return self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][stateConfig.name]

        name = stateConfig.name
        entryAction = stateConfig.entry
        doAction = stateConfig.action
        exitAction = stateConfig.exit
        isInitialState = stateConfig.initial
        isFinalState = stateConfig.final
        isContinuation = stateConfig.continuation
        continuationCountdown = stateConfig.continuationCountdown
        fanInPeriod = stateConfig.fanInPeriod
        fanInGroup = stateConfig.fanInGroup

        return State(name,
                     entryAction,
                     doAction,
                     exitAction,
                     machineName=machineConfig.name,
                     isInitialState=isInitialState,
                     isFinalState=isFinalState,
                     isContinuation=isContinuation,
                     fanInPeriod=fanInPeriod,
                     fanInGroup=fanInGroup,
                     continuationCountdown=continuationCountdown)

    def _getTransition(self, machineConfig, transitionConfig):
        """ Returns a Transition instance based on the machineConfig/transitionConfig

        @param machineConfig: a config._MachineConfig instance
        @param transitionConfig: a config._TransitionConfig instance
        @return: a Transition instance which is a singleton wrt. the FSM instance
        """
        if machineConfig.name in self.machines and \
           transitionConfig.name in self.machines[machineConfig.name][constants.MACHINE_TRANSITIONS_ATTRIBUTE]:
            return self.machines[machineConfig.name][constants.MACHINE_TRANSITIONS_ATTRIBUTE][transitionConfig.name]

        target = self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][transitionConfig.toState.name]
        retryOptions = self._buildRetryOptions(transitionConfig)
        countdown = transitionConfig.countdown
        queueName = transitionConfig.queueName
        taskTarget = transitionConfig.target

        return Transition(transitionConfig.name, target, action=transitionConfig.action,
                          countdown=countdown, retryOptions=retryOptions, queueName=queueName, taskTarget=taskTarget)

    def createFSMInstance(self, machineName, currentStateName=None, instanceName=None, data=None, method='GET',
                          obj=None, headers=None):
        """ Creates an FSMContext instance with non-initialized data

        @param machineName: the name of FSMContext to instantiate, as defined in fsm.yaml
        @param currentStateName: the name of the state to place the FSMContext into
        @param instanceName: the name of the current instance
        @param data: a dict or FSMContext
        @param method: 'GET' or 'POST'
        @param obj: an object that the FSMContext can operate on
        @param headers: a dict of X-Fantasm request headers to pass along in Tasks
        @raise UnknownMachineError: if machineName is unknown
        @raise UnknownStateError: is currentState name is not None and unknown in machine with name machineName
        @return: an FSMContext instance
        """

        try:
            machineConfig = self.config.machines[machineName]
        except KeyError:
            raise UnknownMachineError(machineName)

        initialState = self.machines[machineName][constants.MACHINE_STATES_ATTRIBUTE][machineConfig.initialState.name]

        try:
            currentState = self.pseudoInits[machineName]
            if currentStateName:
                currentState = self.machines[machineName][constants.MACHINE_STATES_ATTRIBUTE][currentStateName]
        except KeyError:
            raise UnknownStateError(machineName, currentStateName)

        retryOptions = self._buildRetryOptions(machineConfig)
        url = machineConfig.url
        queueName = machineConfig.queueName
        taskTarget = machineConfig.target
        useRunOnceSemaphore = machineConfig.useRunOnceSemaphore

        return FSMContext(initialState, currentState=currentState,
                          machineName=machineName, instanceName=instanceName,
                          retryOptions=retryOptions, url=url, queueName=queueName,
                          data=data, contextTypes=machineConfig.contextTypes,
                          method=method,
                          persistentLogging=(machineConfig.logging == constants.LOGGING_PERSISTENT),
                          obj=obj,
                          headers=headers,
                          globalTaskTarget=taskTarget,
                          useRunOnceSemaphore=useRunOnceSemaphore)
class FSMContext(dict):
    """ A finite state machine context instance. """

    def __init__(self, initialState, currentState=None, machineName=None, instanceName=None,
                 retryOptions=None, url=None, queueName=None, data=None, contextTypes=None,
                 method='GET', persistentLogging=False, obj=None, headers=None, globalTaskTarget=None,
                 useRunOnceSemaphore=True):
        """ Constructor

        @param initialState: a State instance
        @param currentState: a State instance
        @param machineName: the name of the fsm
        @param instanceName: the instance name of the fsm
        @param retryOptions: the TaskRetryOptions for the machine
        @param url: the url of the fsm
        @param queueName: the name of the appengine task queue
        @param headers: a dict of X-Fantasm request headers to pass along in Tasks
        @param persistentLogging: if True, use persistent _FantasmLog model
        @param obj: an object that the FSMContext can operate on
        @param globalTaskTarget: the machine-level target configuration parameter
        """
        assert queueName

        super(FSMContext, self).__init__(data or {})
        self.initialState = initialState
        self.currentState = currentState
        self.currentAction = None
        if currentState:
            self.currentAction = currentState.exitAction
        self.machineName = machineName
        self.instanceName = instanceName or self._generateUniqueInstanceName()
        self.queueName = queueName
        self.retryOptions = retryOptions
        self.url = url
        self.method = method
        self.startingEvent = None
        self.startingState = None
        self.contextTypes = constants.PARAM_TYPES.copy()
        if contextTypes:
            self.contextTypes.update(contextTypes)
        self.logger = Logger(self, obj=obj, persistentLogging=persistentLogging)
        self.__obj = obj
        self.headers = headers
        self.globalTaskTarget = globalTaskTarget
        self.useRunOnceSemaphore = useRunOnceSemaphore

        # the following is monkey-patched from handler.py for 'immediate mode'
        from google.appengine.api.taskqueue.taskqueue import Queue
        self.Queue = Queue # pylint: disable-msg=C0103

    INSTANCE_NAME_DTFORMAT = '%Y%m%d%H%M%S'

    def _generateUniqueInstanceName(self):
        """ Generates a unique instance name for this machine.

        @return: a FSMContext instanceName that is (pretty darn likely to be) unique
        """
        utcnow = datetime.datetime.utcnow()
        dateStr = utcnow.strftime(self.INSTANCE_NAME_DTFORMAT)
        randomStr = ''.join(random.sample(constants.CHARS_FOR_RANDOM, 6))
        # note this construction is important for getInstanceStartTime()
        return '%s-%s-%s' % (self.machineName, dateStr, randomStr)

    def dispatch(self, event, obj):
        """ The main entry point to move the machine according to an event.

        @param event: a string event to dispatch to the FSMContext
        @param obj: an object that the FSMContext can operate on
        @return: an event string to dispatch to the FSMContext
        """

        self.__obj = self.__obj or obj # hold the obj object for use during this context

        # store the starting state and event for the handleEvent() method
        self.startingState = self.currentState
        self.startingEvent = event

        nextEvent = None
        try:
            nextEvent = self.currentState.dispatch(self, event, obj)

            if obj.get(constants.FORKED_CONTEXTS_PARAM):
                # pylint: disable-msg=W0212
                # - accessing the protected method is fine here, since it is an instance of the same class
                tasks = []
                for context in obj[constants.FORKED_CONTEXTS_PARAM]:
                    context[constants.STEPS_PARAM] = int(context.get(constants.STEPS_PARAM, '0')) + 1
                    task = context.queueDispatch(nextEvent, queue=False)
                    if task: # fan-in magic
                        if not task.was_enqueued: # fan-in always queues
                            tasks.append(task)

                try:
                    if tasks:
                        transition = self.currentState.getTransition(nextEvent)
                        _queueTasks(self.Queue, transition.queueName, tasks)

                except (TaskAlreadyExistsError, TombstonedTaskError):
                    # unlike a similar block in self.continutation, this is well off the happy path
                    self.logger.critical(
                                     'Unable to queue fork Tasks %s as it/they already exists. (Machine %s, State %s)',
                                     [task.name for task in tasks if not task.was_enqueued],
                                     self.machineName,
                                     self.currentState.name)

            if nextEvent:
                self[constants.STEPS_PARAM] = int(self.get(constants.STEPS_PARAM, '0')) + 1

                try:
                    self.queueDispatch(nextEvent)

                except (TaskAlreadyExistsError, TombstonedTaskError):
                    # unlike a similar block in self.continutation, this is well off the happy path
                    #
                    # FIXME: when this happens, it means there was failure shortly after queuing the Task, or
                    #        possibly even with queuing the Task. when this happens there is a chance that
                    #        two states in the machine are executing simultaneously, which is may or may not
                    #        be a good thing, depending on what each state does. gracefully handling this
                    #        exception at least means that this state will terminate.
                    # NOTE:  This happens most often due to oddities in the Task Queue system. E.g., task
                    #        queue will raise an exception while doing a BulkAdd or a TransientError occurs,
                    #        however despite this error, the task was enqueued (and the task name was
                    #        registered in the tombstone system). That is, it seems that most often, the
                    #        error that leads to this message does not mean that a particular machine did
                    #        not continue; it probably always does continue. However, not that the particular
                    #        state that issues this particular warning was triggered twice, so any side-effect
                    #        (e.g., sending an email) would have occurred twice. Remember it is very important
                    #        for your machine states to be idemopotent meaning they have to protect against
                    #        this situation on their own as the task queue system itself is distributed and
                    #        definitely not perfect.
                    self.logger.warning('Unable to queue next Task as it already exists. (Machine %s, State %s)',
                                     self.machineName,
                                     self.currentState.name)

            else:
                # if we're not in a final state, emit a log message
                # FIXME - somehow we should avoid this message if we're in the "last" step of a continuation...
                if not self.currentState.isFinalState and not obj.get(constants.TERMINATED_PARAM):
                    self.logger.critical('Non-final state did not emit an event. Machine has terminated in an ' +
                                     'unknown state. (Machine %s, State %s)' %
                                     (self.machineName, self.currentState.name))
                # if it is a final state, then dispatch the pseudo-final event to finalize the state machine
                elif self.currentState.isFinalState and self.currentState.exitAction:
                    self[constants.STEPS_PARAM] = int(self.get(constants.STEPS_PARAM, '0')) + 1
                    self.queueDispatch(FSM.PSEUDO_FINAL)

        except HaltMachineError, e:
            if e.level is not None and e.message:
                self.logger.log(e.level, e.message)
            return None # stop the machine
        except Exception, e:
            level = self.logger.error
            if e.__class__ in TRANSIENT_ERRORS:
                level = self.logger.warn
            level("FSMContext.dispatch is handling the following exception:", exc_info=True)
            self._handleException(event, obj)

        return nextEvent
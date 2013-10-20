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

from hxfsm.state        import State
from hxfsm.transition   import Transition
from hxfsm              import constants
from hxfsm.exceptions   import UnknownEventError, UnknownStateError, UnknownMachineError, \
                               HaltMachineError

class FSM(object):
    """ An FSM creation factory. This is primarily responsible for translating machine
    configuration information (config.currentConfiguration()) into singleton States and Transitions as per [1]
    """

    FSM_INIT_STATE  = 'fsm-init-state'
    machines        = {}

    def __init__(self, machineName, statesList, transList):
        """ Constructor which either initializes Finite States Machine

        @param machineName  : FSM  machine name, must be unique
        @param statesList   : all states in FSM machine
        @param transList    : all transition in FSM machine

        """
        import logging
        logging.info("Initializing FSM machine.")                

        # FSM machine name must be unique!
        if machineName in FSM.machines:
            logging.info("FSM name " + machineName + "already used!")
            return

        # Use dict to store a FSM machine, it typically looks like as below
        # machine looks like {'states'      : {'stateName1': stateObj1,
        #                                      'stateName2': stateObj2},
        #                     'transitions' : {'transitionName1': transObj1,
        #                                      'transitionName2': transObj2}}
        FSM.machines[machineName] = {constants.MACHINE_STATES_ATTRIBUTE: {}, constants.MACHINE_TRANSITIONS_ATTRIBUTE: {}}
        machine         = FSM.machines[machineName]
        machineStates   = machine[constants.MACHINE_STATES_ATTRIBUTE]
        machineTrans    = machine[constants.MACHINE_TRANSITIONS_ATTRIBUTE]

        # add states to FSM.machines
        def _addState(state):
            # mark the initial state
            if state.isInitialState:
                machineStates[FSM.FSM_INIT_STATE] = state
            if state.name == FSM.FSM_INIT_STATE:
                logging.info("state name should not be %s.", FSM.FSM_INIT_STATE)
            machineStates[state.name] = state
        map(_addState, statesList)

        # add transition to FSM.machines
        def _addTranstion(transition):
            machineTrans[transition.name] = transition
            machineStates[transition.source.name].addTransition(transition, transition.event) 
        map(_addTranstion, transList)

    def createFSMInstance(self, machineName, currentStateName=None, instanceName=None, data=None, method='GET',
                          obj=None):
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
            machine = FSM.machines[machineName]
        except KeyError:
            raise UnknownMachineError(machineName)

        initialState = machine[constants.MACHINE_STATES_ATTRIBUTE][FSM.FSM_INIT_STATE]
        
        try:
            currentState = initialState
            if currentStateName:
                currentState = machine[constants.MACHINE_STATES_ATTRIBUTE][currentStateName]
        except KeyError:
            raise UnknownStateError(machineName, currentStateName)

        return FSMContext(initialState, currentState=currentState,
                          machineName=machineName, instanceName=instanceName,
                          data=data, obj=obj)
                        
class FSMContext(dict):
    """ A finite state machine context instance. """

    def __init__(self, initialState, currentState=None, machineName=None, instanceName=None,
                 data=None, obj=None):
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

        self.startingEvent = None
        self.startingState = None

        self.__obj = obj

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
        '''
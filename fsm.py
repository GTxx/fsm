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
        for state in statesList:    
            # mark the initial state
            if state.isInitialState:
                machineStates[FSM.FSM_INIT_STATE] = state
            if state.name == FSM.FSM_INIT_STATE:
                logging.info("state name should not be %s.", FSM.FSM_INIT_STATE)

            machineStates[state.name] = state

        # add transition to FSM.machines
        for transition in transList:
            machineTrans[transition.name] = transition
            machineStates[transition.source.name].addTransition(transition, transition.event) 

    def createFSMInstance(machineName, currentStateName=None, instanceName=None, data=None, 
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

    contexts = {}
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

        FSMContext.contexts[self.instanceName] = {'initialState':initialState.name, 'currentState' : currentState.name, \
                                             'machineName' : machineName}    # TODO: save FSMContext instance in DB.

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

    def dispatch(self, event):
        """ The main entry point to move the machine according to an event.

        @param event: a string event to dispatch to the FSMContext
        @param obj: an object that the FSMContext can operate on
        @return: an event string to dispatch to the FSMContext
        """

        #self.__obj = self.__obj or obj # hold the obj object for use during this context

        # store the starting state and event for the handleEvent() method
        self.startingState = self.currentState
        self.startingEvent = event

        nextEvent = None
        try:
            nextEvent = self.currentState.dispatch(self, event)

        except HaltMachineError as e:
            if e.level is not None and e.message:
                self.logger.log(e.level, e.message)
            return None # stop the machine


        return nextEvent
        

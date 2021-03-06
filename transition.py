""" Fantasm: A taskqueue-based Finite State Machine for App Engine Python

Docs and examples: http://code.google.com/p/fantasm/

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
"""
#from state import State
#from exceptions import  HaltMachineError 
from hxfsm.exceptions import HaltMachineError

class Transition(object):
    """ A transition object for a machine. """

    def __init__(self, name, source, target, event, action=None):
        """ Constructor

        @param name: the name of the Transition instance
        @param source: a State instance
        @param target: a State instance
        @param action: the optional action for a state
        @param countdown: the number of seconds to wait before firing this transition. Default 0.
        @param retryOptions: the TaskRetryOptions for this transition
        @param queueName: the name of the queue to Queue into
        @param taskTarget: the target for tasks created for this transition
        """


        self.source = source
        self.target = target
        self.event = event
        self.name = name
        self.action = action

    # W0613:144:Transition.execute: Unused argument 'obj'
    # args are present for a future(?) transition action
    #def execute(self, context, obj): # pylint: disable-msg=W0613
        """ Moves the machine to the next state.

        @param context: an FSMContext instance
        @param obj: an object that the Transition can operate on

        Notes: This function has side effect and changes context

        """
        '''
        if self.action:
            try:
                self.action.execute(context, obj)
            except HaltMachineError:
                raise # let it bubble up quietly
            except Exception, e:
                level = context.logger.error
                if e.__class__ in TRANSIENT_ERRORS:
                    level = context.logger.warn
                level('Error processing action for transition. (Machine %s, Transition %s, Action %s)',
                      context.machineName, self.name, self.action.__class__)
                raise
        context.currentState = self.target
'''
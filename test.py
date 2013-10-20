from hxfsm.state 		import State
from hxfsm.transition 	import Transition
from hxfsm.fsm			import FSM
from hxfsm.fsm 			import FSMContext 

put_up_order 		= State("put_up_order", isInitialState = True) 
paid				= State("paid") 
product_shipping 	= State("product_shipping")
waiting_receive		= State("waiting_receive")
done				= State('done', isFinalState = True) 
statesList =  [put_up_order, paid, product_shipping, waiting_receive, done]

put_up_to_paid = Transition("put_up_to_paid", put_up_order, paid, "pay money")
paid_to_shipping = Transition("paid_to_shipping", paid, product_shipping, "get product from repository")
shipping_to_wait = Transition("shipping_to_wait", product_shipping, waiting_receive, "shipping out")
waiting_to_done = Transition("waiting_to_done", waiting_receive, done, "product received")
transList= [put_up_to_paid, paid_to_shipping, shipping_to_wait, waiting_to_done]

fsmmachine=FSM("order", [put_up_order, paid, product_shipping, waiting_receive, done], \
	[put_up_to_paid, paid_to_shipping, shipping_to_wait, waiting_to_done])


#fsmcontext = FSM.createFSMInstance('order')


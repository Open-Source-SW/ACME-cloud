#
#	EventManager.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	Managing event handlers and events
#

from __future__ import annotations
import threading
from typing import Callable, Any, cast
from ..etc import Utils as Utils

_running:bool = False

# TODO: create/delete each resource to count! resourceCreate(ty)

# TODO move event creations from here to the resp modules.




#########################################################################
#
#	Event class.
#


class Event(list):	# type:ignore[type-arg]
	"""Event subscription.

	A list of callable methods. Calling an instance of Event will cause a
	call to each function in the list in ascending order by index. 
	It supports all methods from its base class (list), so use append() and remove()
	to add and remove functions.

	An event is raised by calling the event: anEvent(anArgument). It may have an
	arbitrary number of arguments which are passed to the functions.

	The function will be called in a separate thread in order to prevent waiting
	for the returns. This might lead to some race conditions, so the synchronizations
	must be done inside the functions.

	Attention: Since the parent class is *list* `isInstance(obj, list)` will yield True.
	"""

	def __init__(self, runInBackground:bool=True, manager:EventManager=None):
		self.runInBackground = runInBackground
		self.manager = manager


	def __call__(self, *args:Any, **kwargs:Any) -> None:

		def _callThread(*args:Any, **kwargs:Any) -> None:
			"""	Call all attached function for this event object.
			"""
			for function in self:
				function(*args, **kwargs)


		if not self.manager._running:
			return
		if self.runInBackground:
			# Call the handlers in a thread so that we don't block everything
			thread = threading.Thread(target=_callThread, args=args, kwargs=kwargs)
			thread.setDaemon(True)		# Make the thread a daemon of the main thread
			thread.start()
			Utils.renameCurrentThread(thread=thread)
		else:
			_callThread(*args, **kwargs)


	def __repr__(self) -> str:
		return f'Event({list.__repr__(self)})' 



class EventManager(object):
	"""Event topics are added as new methods of the handler class with the given name and can be raised by calling those new methods, e.g.

		- manager.addEvent("someName") : add new event topic
		- manager.addHandler(manager.someName, handlerFunction) : add an event handler
		- handler.someName() : raises the event
	"""

	def __init__(self) -> None:
		self._running = True

	def shutdown(self) -> bool:
		self._running = False
		self.removeAllEvents()
		return True

	#########################################################################

	def addEvent(self, name:str, runInBackground:bool=True) -> Event:
		"""	Create and add a new event.
		"""
		if not hasattr(self, name):
			setattr(self, name, Event(runInBackground=runInBackground, manager=self))
		return cast(Event, getattr(self, name))


	def removeEvent(self, name:str) -> None:
		"""	Remove an event by name.
		"""
		if hasattr(self, name):
			delattr(self, name)
	
	
	def removeAllEvents(self) -> None:
		"""	Remove all registered events.
		"""
		for n in list(vars(self)):
			if isinstance(self.__dict__[n], Event):
				self.removeEvent(n)


	def hasEvent(self, name:str) -> bool:
		"""	Check whether an event exists.
		"""
		return name in self.__dict__


	def addHandler(self, event:Event|list[Event], func:Callable) -> None:		# type:ignore[type-arg]
		"""	Add a new event handler for an `event` or a list of events.
		"""
		list(map(lambda e: e.append(func), [event] if isinstance(event, Event) else event))


	def removeHandler(self, event:Event|list[Event], func:Callable) -> None:	# type:ignore[type-arg]
		"""	Remove an event handler from an `event` or a list of events.
		"""
		list(map(lambda e: e.remove(func), [event] if isinstance(event, Event) else event))
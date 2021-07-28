#
#	TS_OL.py
#
#	(c) 2021 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	ResourceType: oldest (virtual resource) for timeSeries
#

from __future__ import annotations
from typing import cast, Optional
from Types import ResourceTypes as T, ResponseCode as RC, Result, JSON, CSERequest
import CSE
from .Resource import *
from Logging import Logging as L


class TS_OL(Resource):

	# Specify the allowed child-resource types
	allowedChildResourceTypes:list[T] = [ ]


	def __init__(self, dct:JSON=None, pi:str=None, create:bool=False) -> None:
		super().__init__(T.TS_OL, dct, pi, create=create, inheritACP=True, readOnly=True, rn='ol', isVirtual=True)


	def handleRetrieveRequest(self, request:CSERequest=None, id:str=None, originator:str=None) -> Result:
		""" Handle a RETRIEVE request. Return resource """
		if L.isDebug: L.logDebug('Retrieving oldest TSI from TS')
		if (r := self._getOldest()) is None:
			return Result(rsc=RC.notFound, dbg='no instance for <oldest>')
		return Result(resource=r)


	def handleCreateRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		""" Handle a CREATE request. Fail with error code. """
		return Result(rsc=RC.operationNotAllowed, dbg='operation not allowed for <oldest> resource type')


	def handleUpdateRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		""" Handle a UPDATE request. Fail with error code. """
		return Result(rsc=RC.operationNotAllowed, dbg='operation not allowed for <oldest> resource type')


	def handleDeleteRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		""" Handle a DELETE request. Delete the latest resource. """
		if L.isDebug: L.logDebug('Deleting oldest TSI from TS')
		if (r := self._getOldest()) is None:
			return Result(rsc=RC.notFound, dbg='no instance for <oldest>')
		return CSE.dispatcher.deleteResource(r, originator, withDeregistration=True)


	def _getOldest(self) -> Optional[Resource]:
		pi = self['pi']
		rs = []
		if (parentResource := CSE.dispatcher.retrieveResource(pi).resource) is not None:
			rs = parentResource.timeSeriesInstances()			# ask parent for all TSI
		return cast(Resource, rs[0]) if len(rs) > 0 else None
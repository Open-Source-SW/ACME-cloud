#
#	REQ.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	ResourceType: Request
#
""" Request (REQ) resource type. """

from __future__ import annotations
from typing import Optional, Dict, Any

from configparser import ConfigParser

from ..etc.Types import AttributePolicyDict, ResourceTypes, RequestStatus, CSERequest, JSON
from ..etc.ResponseStatusCodes import ResponseStatusCode, UNABLE_TO_RECALL_REQUEST
from ..helpers.TextTools import setXPath	
from ..etc.DateUtils import getResourceDate
from ..runtime.Configuration import Configuration, ConfigurationError
from ..resources.Resource import Resource
from ..resources.CSEBase import getCSE
from ..resources import Factory	# attn: circular import
from ..runtime import CSE


class REQ(Resource):
	""" Request (REQ) resource type. """

	# Specify the allowed child-resource types
	_allowedChildResourceTypes = [ ResourceTypes.SUB ]
	""" The allowed child-resource types. """

	# Attributes and Attribute policies for this Resource Class
	# Assigned during startup in the Importer
	_attributes:AttributePolicyDict = {		
		# Common and universal attributes
		'rn': None,
		'ty': None,
		'ri': None,
		'pi': None,
		'ct': None,
		'lt': None,
		'et': None,
		'lbl': None,
		'cstn': None,
		'acpi':None,
		'daci': None,

		# Resource attributes
		'op': None,
		'tg': None,
		'org': None,
		'rid': None,
		'mi': None,
		'pc': None,
		'rs': None,
		'ors': None
	}
	"""	Attributes and `AttributePolicy` for this resource type. """


	def __init__(self, dct:Optional[JSON] = None, 
					   pi:Optional[str] = None, 
					   create:Optional[bool] = False) -> None:
		super().__init__(ResourceTypes.REQ, dct, pi, create = create)


	def willBeDeactivated(self, originator: str, parentResource: Resource) -> None:
		match self['rs']:
			case RequestStatus.PENDING:
				# We cannot really cancel a request. This is for further study.
				raise UNABLE_TO_RECALL_REQUEST('Unable to cancel PENDING request')
			case RequestStatus.FORWARDED:
				raise UNABLE_TO_RECALL_REQUEST('Unable to cancel FORWARDED request')
			case RequestStatus.PARTIALLY_COMPLETED:
				raise UNABLE_TO_RECALL_REQUEST('Unable to cancel PARTIALLY_COMPLETED request')
		return super().willBeDeactivated(originator, parentResource)
	
	
	@staticmethod
	def createRequestResource(request:CSERequest) -> Resource:
		"""	Create an initialized <request> resource.

			Args:
				request: The request to create the resource for.

			Return:
				The created REQ resource.
		"""

		# Check if a request expiration ts has been set in the request
		if request.rqet:
			et = request.rqet	# This is already an ISO8601 timestamp
		
		# Check the rp(ts) argument
		elif request._rpts:
			et = request._rpts
		
		# otherwise get the request's et from the configuration
		else:	
			et = getResourceDate(offset = Configuration.resource_req_et)


		# Build the REQ resource from the original request
		dct:Dict[str, Any] = {
			'm2m:req' : {
				'et': et,
				'lbl': [ request.originator ],
				'op': request.op,
				'tg': request.id,
				'org': request.originator,
				'rid': request.rqi,
				'mi': {
					'ty': request.ty,
					'ot': getResourceDate(),
					'rqet': request.rqet,
					'rset': request.rset,
					'rt': { 
						'rtv': request.rt
					},
					'rp': request.rp,
					'rcn': request.rcn,
					'fc': {
						'fu': request.fc.fu,
						'fo': request.fc.fo,
					},
					'drt': request.drt,
					'rvi': request.rvi if request.rvi else CSE.releaseVersion,
					'vsi': request.vsi,
					'sqi': request.sqi,
				},
				'rs': RequestStatus.PENDING,
				'ors': {
					'rsc': ResponseStatusCode.ACCEPTED,
					'rqi': request.rqi,
				}
		}}

		# add handlings, conditions and attributes from filter
		for k,v in { **request.fc.criteriaAttributes(), **request.fc.attributes}.items():
			setXPath(dct, f'm2m:req/mi/fc/{k}', v, True)

		# add content
		if request.pc and len(request.pc) > 0:
			setXPath(dct, 'm2m:req/pc', request.pc, True)

		# calculate and assign rtu for rt
		if (rtu := request.rtu) and len(rtu) > 0:
			setXPath(dct, 'm2m:req/mi/rt/nu', [ u for u in rtu if len(u) > 0] )

		return Factory.resourceFromDict(dct, pi = CSE.cseRi, ty = ResourceTypes.REQ)


def readConfiguration(parser:ConfigParser, config:Configuration) -> None:

	#	Defaults for Request Resources

	config.resource_req_et = parser.getint('resource.req', 'expirationTime', fallback = 60)


def validateConfiguration(config:Configuration, initial:Optional[bool] = False) -> None:

	if config.resource_req_et <= 0:
		raise ConfigurationError(r'Configuration Error: [i]\[resource.req]:expirationTime[/i] must be > 0')

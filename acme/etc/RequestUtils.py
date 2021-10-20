#
#	RequestUtils.py
#
#	(c) 2021 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	This module contains various utilty functions that are used to work with requests and responses
#


from __future__ import annotations
import cbor2, json
from typing import Any, cast, Dict
from urllib.parse import urlparse, urlunparse, parse_qs, urlunparse, urlencode
from ..etc.DateUtils import waitFor

from .Types import CSERequest, ContentSerializationType, JSON, RequestType, ResponseStatusCode as RC, Result, ResourceTypes as T, Operation
from ..etc import DateUtils
from .Constants import Constants as C
from ..services.Logging import Logging as L
from ..helpers import TextTools


def serializeData(data:JSON, ct:ContentSerializationType) -> str|bytes|JSON:
	"""	Serialize a dictionary, depending on the serialization type.
	"""
	if ct == ContentSerializationType.PLAIN:
		return data
	encoder = json if ct == ContentSerializationType.JSON else cbor2 if ct == ContentSerializationType.CBOR else None
	if not encoder:
		return None
	return encoder.dumps(data)	# type:ignore[no-any-return]


def deserializeData(data:bytes, ct:ContentSerializationType) -> JSON:
	"""	Deserialize data into a dictionary, depending on the serialization type.
		If the len of the data is 0 then an empty dictionary is returned. 
	"""
	if len(data) == 0:
		return {}
	if ct == ContentSerializationType.JSON:
		return cast(JSON, json.loads(TextTools.removeCommentsFromJSON(data.decode('utf-8'))))
	elif ct == ContentSerializationType.CBOR:
		return cast(JSON, cbor2.loads(data))
	return None


def toHttpUrl(url:str) -> str:
	"""	Make the `url` a valid http URL (escape // and ///)
		and return it.
	"""
	u = list(urlparse(url))
	if u[2].startswith('///'):
		u[2] = f'/_{u[2][2:]}'
		url = urlunparse(u)
	elif u[2].startswith('//'):
		u[2] = f'/~{u[2][1:]}'
		url = urlunparse(u)
	return url


def determineSerialization(url:str, csz:list[str]=None, defaultSerialization:ContentSerializationType=None) -> ContentSerializationType:
	"""	Determine the type of serialization for a notification from either the `url`'s `ct` query parameter,
		or the given list of `csz`(contentSerializations, attribute of a target AE/CSE), or the CSE's default serialization.
	"""
	ct = None
	scheme = None

	# Dissect url and check whether ct is an argumemnt. If yes, then remove it
	# and store it to check later
	uu = list(urlparse(url))
	qs = parse_qs(uu[4], keep_blank_values=True)
	if ('ct' in qs):
		ct = qs.pop('ct')[0]	# remove the ct=
		uu[4] = urlencode(qs, doseq=True)
		url = urlunparse(uu)	# reconstruct url w/o ct
	scheme = uu[0]

	# Check scheme first
	# TODO should this really be in this function?
	if scheme not in C.supportedSchemes:
		L.isWarn and L.logWarn(f'URL scheme {scheme} not supported')
		return None	# Scheme not supported

	if ct:
		# if ct is given then check whether it is supported, 
		# otherwise ignore this url
		if ct not in C.supportedContentSerializationsSimple:
			return None	# Requested serialization not supported
		return ContentSerializationType.to(ct)

	elif csz:
		# if csz is given then build an intersection between the given list and
		# the list of supported serializations. Then take the first one
		# as the one to use.
		common = [x for x in csz if x in C.supportedContentSerializations]	# build intersection, keep the old sort order
		if len(common) == 0:
			return None
		return ContentSerializationType.to(common[0]) # take the first
	
	# Just use default serialization.
	return defaultSerialization


def requestFromResult(inResult:Result, originator:str=None, ty:T=None, op:Operation=None, isResponse:bool=False) -> Result:
	"""	Convert a response request to a new *Result* object and create a new dictionary in *Result.data*
		with the full Response structure. Recursively do this if the *embeddedRequest* is also
		a full Request or Response.
	"""
	from ..services import CSE

	req:JSON = {}

	# Assign the From and to of the request. An assigned originator has priority for this
	if originator:
		req['fr'] = CSE.cseCsi if isResponse else originator
		req['to'] = inResult.request.id if inResult.request.id else originator
	elif inResult.request.headers.originator:
		req['fr'] = CSE.cseCsi if isResponse else inResult.request.headers.originator
		req['to'] = inResult.request.id if inResult.request.id else inResult.request.headers.originator    
	else:
		req['fr'] = CSE.cseCsi
		req['to'] = inResult.request.id if inResult.request.id else CSE.cseCsi

	# req['to'] = inResult.request.id if inResult.request.id else inResult.request.headers.originator # else 'non-onem2m-entity'

	if inResult.request.headers.originatingTimestamp:
		req['ot'] = inResult.request.headers.originatingTimestamp
	if inResult.rsc and inResult.rsc != RC.UNKNOWN:
		req['rsc'] = int(inResult.rsc)
	if op:
		req['op'] = int(op)
	if ty:
		req['ty'] = int(ty)
	if inResult.request.headers.requestIdentifier:					# copy from the original request
		req['rqi'] = inResult.request.headers.requestIdentifier
	if inResult.request.headers.releaseVersionIndicator:			# copy from the original request
		req['rvi'] = inResult.request.headers.releaseVersionIndicator
	if inResult.request.headers.vendorInformation:					# copy from the original request
		req['vsi'] = inResult.request.headers.vendorInformation
	
	# Add additional parameters
	if inResult.request.parameters:
		if (ec := inResult.request.parameters.get(C.hfEC)):			# Event Category, copy from the original request
			req['ec'] = ec
	
	# If the response contains a request (ie. for polling), then add that request to the pc
	pc = None
	# L.isDebug and L.logDebug(inResult)

	if inResult.embeddedRequest:
		if inResult.embeddedRequest.originalRequest:
			pc = inResult.embeddedRequest.originalRequest
		else:
			pc = cast(JSON, requestFromResult(Result(request=inResult.embeddedRequest)).data)
		# L.isDebug and L.logDebug(pc)

		# pc = inResult.embeddedRequest.originalRequest if inResult.embeddedRequest.originalRequest else inResult.embeddedRequest.pc
		# pc = inResult.embeddedRequest.pc if inResult.embeddedRequest.pc is not None else inResult.embeddedRequest.originalRequest
		# pc = inResult.embeddedRequest.originalRequest
	else:
		# construct and serialize the data as JSON/dictionary. Encoding to JSON or CBOR is done later
		pc = inResult.toData(ContentSerializationType.PLAIN)	#  type:ignore[assignment]
	if pc:
		# if the request/result is actually an incoming request targeted to the receiver, then the
		# whole request must be embeded as a "m2m:rqp" request.
		if inResult.embeddedRequest and inResult.embeddedRequest.requestType == RequestType.REQUEST:
			req['pc'] = { 'm2m:rqp' : pc }
		else:
			req['pc'] = pc
	
	return Result(status=True, data=req, resource=inResult.resource, request=inResult.request, embeddedRequest=inResult.embeddedRequest, rsc=inResult.rsc)



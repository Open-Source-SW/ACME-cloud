#
#	SecurityManager.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#

"""	This module implements the SecurityManager entity.
"""


from __future__ import annotations
from typing import List, cast, Optional, Any, Tuple

import ssl

from ..etc.Types import JSON, ResourceTypes, Permission, Result, CSERequest
from ..etc.ResponseStatusCodes import ResponseException, BAD_REQUEST, ORIGINATOR_HAS_NO_PRIVILEGE, NOT_FOUND, INTERNAL_SERVER_ERROR
from ..etc.ACMEUtils import isSPRelative, toCSERelative, getIdFromOriginator
from ..helpers.TextTools import findXPath, simpleMatch
from ..runtime import CSE
from ..runtime.Configuration import Configuration
from ..resources.Resource import Resource
from ..resources.PCH import PCH
from ..resources.PCH_PCU import PCH_PCU
from ..resources.ACP import ACP
from ..resources.ACPAnnc import ACPAnnc
from ..runtime.Logging import Logging as L


class SecurityManager(object):
	"""	This manager entity handles access to resources and requests.
	"""

	__slots__ = (
		'enableACPChecks',
		'fullAccessAdmin',
		'useTLSHttp',
		'verifyCertificateHttp',
		'tlsVersionHttp',
		'caCertificateFileHttp',
		'caPrivateKeyFileHttp',
		'useTlsMqtt',
		'verifyCertificateMqtt',
		'caCertificateFileMqtt',
		'usernameMqtt',
		'passwordMqtt',
		'allowedCredentialIDsMqtt',
		'httpBasicAuthFile',
		'httpTokenAuthFile',
		'httpBasicAuthData',
		'httpTokenAuthData',
		'useTLSWs',
		'verifyCertificateWs',
		'tlsVersionWs',
		'caCertificateFileWs',
		'caPrivateKeyFileWs',
		'slashCseOriginator',
	)


	def __init__(self) -> None:

		# Get the configuration settings
		self._assignConfig()
		self._readHttpBasicAuthFile()
		self._readHttpTokenAuthFile()

		# Add a handler when the CSE is reset
		CSE.event.addHandler(CSE.event.cseReset, self.restart)	# type: ignore

		# Add handler for configuration updates
		CSE.event.addHandler(CSE.event.configUpdate, self.configUpdate)				# type: ignore

		L.isInfo and L.log('SecurityManager initialized')
		if self.enableACPChecks:
			L.isInfo and L.log('ACP checking ENABLED')
		else:
			L.isInfo and L.log('ACP checking DISABLED')


	def shutdown(self) -> bool:
		L.isInfo and L.log('SecurityManager shut down')
		return True
	

	def restart(self, name:str) -> None:
		"""	Restart the Security manager service.
		"""
		self._assignConfig()
		self._readHttpBasicAuthFile()
		self._readHttpTokenAuthFile()
		L.logDebug('SecurityManager restarted')


	def _assignConfig(self) -> None:
		"""	Assign configurations.
		"""

		self.enableACPChecks 			= Configuration.get('cse.security.enableACPChecks')
		self.fullAccessAdmin			= Configuration.get('cse.security.fullAccessAdmin')

		# TLS configurations (http)
		self.useTLSHttp 				= Configuration.get('http.security.useTLS')
		self.verifyCertificateHttp		= Configuration.get('http.security.verifyCertificate')
		self.tlsVersionHttp				= Configuration.get('http.security.tlsVersion').lower()
		self.caCertificateFileHttp		= Configuration.get('http.security.caCertificateFile')
		self.caPrivateKeyFileHttp		= Configuration.get('http.security.caPrivateKeyFile')

		# HTTP authentication
		self.httpBasicAuthFile			= Configuration.get('http.security.basicAuthFile')
		self.httpTokenAuthFile			= Configuration.get('http.security.tokenAuthFile')

		# TLS and other configuration (mqtt)
		self.useTlsMqtt 				= Configuration.get('mqtt.security.useTLS')
		self.verifyCertificateMqtt		= Configuration.get('mqtt.security.verifyCertificate')
		self.caCertificateFileMqtt		= Configuration.get('mqtt.security.caCertificateFile')
		self.usernameMqtt				= Configuration.get('mqtt.security.username')
		self.passwordMqtt				= Configuration.get('mqtt.security.password')
		self.allowedCredentialIDsMqtt	= Configuration.get('mqtt.security.allowedCredentialIDs')

		# TLS configurations (websocket)
		self.useTLSWs	 				= Configuration.get('websocket.security.useTLS')
		self.verifyCertificateWs		= Configuration.get('websocket.security.verifyCertificate')
		self.tlsVersionWs				= Configuration.get('websocket.security.tlsVersion').lower()
		self.caCertificateFileWs		= Configuration.get('websocket.security.caCertificateFile')
		self.caPrivateKeyFileWs			= Configuration.get('websocket.security.caPrivateKeyFile')

		# Optimizations
		self.slashCseOriginator			= f'/{CSE.cseOriginator}'



	def configUpdate(self, name:str, 
						   key:Optional[str] = None,
						   value:Any = None) -> None:
		"""	Handle configuration updates.

			Args:
				name: The name of the configuration section.
				key: The key of the configuration value.
				value: The new value of the configuration value.
		"""
		if key not in ( 'cse.security.enableACPChecks', 
						'cse.security.fullAccessAdmin',
						'http.security.useTLS',
						'http.security.verifyCertificate',
						'http.security.tlsVersion',
						'http.security.caCertificateFile',
						'http.security.caPrivateKeyFile',
						'http.security.basicAuthFile',
						'mqtt.security.useTLS',
						'mqtt.security.verifyCertificate',
						'mqtt.security.caCertificateFile',
						'mqtt.security.username',
						'mqtt.security.password',
						'mqtt.security.allowedCredentialIDs',
						'websocket.security.useTLS',
						'websocket.security.verifyCertificate',
						'websocket.security.tlsVersion',
						'websocket.security.caCertificateFile',
						'websocket.security.caPrivateKeyFile',
					  ):
			return
		self._assignConfig()
		self._readHttpBasicAuthFile()
		self._readHttpTokenAuthFile()


	###############################################################################################


	def hasAccess(self, originator:str, 
						resource:Resource, 
						requestedPermission:Permission, 
						ty:Optional[ResourceTypes] = None, 
						parentResource:Optional[Resource] = None) -> bool:
		""" Test whether an originator has access to a resource for the requested permission.
		
			Args:
				originator: The originator to check for.
				resource: The target resource of a request.
				requestedPermission: The persmission to test.
				ty: Mandatory for CREATE, else optional. The type of the resoure that is about to be created.
				parentResource: Optional, the parent resource of a target resource.
			Return:
				Boolean indicating access.
		"""

		def _checkACPI(originator:str, acpRi:str, requestedPermission:Permission, ty:ResourceTypes) -> bool:
			""" Check the access control policy for a single ACP resource.

				Args:
					originator: The originator to check for.
					acpRo: The resourceID of the ACP resource.
					requestedPermission: The permission to check.
					ty: The resource type to check for.

				Return:
					Boolean indicating access.
			"""
			try:
				if not (acp := CSE.dispatcher.retrieveResource(acpRi)):	# resource could be on another CSE
					L.isDebug and L.logDebug(f'ACP resource not found: {acpRi}')
					return False
				if self.checkSingleACPPermission(cast(ACP, acp), originator, requestedPermission, ty):
					L.isDebug and L.logDebug('Permission granted')
					return True
			except ResponseException as e:
				L.isDebug and L.logDebug(f'ACP resource not found: {acpRi}: {e.dbg}')
				return False
			return False


		#  Do or ignore the check
		if not self.enableACPChecks:
			return True
		
		#
		# grant full access to the CSE originator
		#
		if originator is None or originator == CSE.cseOriginator or originator.endswith(self.slashCseOriginator) and self.fullAccessAdmin:
			L.isDebug and L.logDebug('Request from CSE Originator. OK.')
			return True

		#
		# Always allow the CSE to NOTIFY
		#
		if requestedPermission == Permission.NOTIFY and originator == CSE.cseCsi:
			L.isDebug and L.logDebug(f'NOTIFY permission granted for CSE: {originator}')
			return True

		#
		# Preparation: Remove CSE-ID if this is the same CSE
		#
		if isSPRelative(originator) and originator.startswith(CSE.cseCsiSlash):
			L.isDebug and L.logDebug(f'Originator: {originator} is registered to same CSE. Converting it to CSE-Relative format.')
			originator = toCSERelative(originator)
			L.isDebug and L.logDebug(f'Converted originator: {originator}')

		#
		#	Check parameters
		#
		if not requestedPermission or not (0 <= requestedPermission <= Permission.ALL):
			L.isWarn and L.logWarn('RequestedPermission must not be None, and between 0 and 63')
			return False

		#
		# Some Separate	tests for some types
		#
		if ty is not None:	# ty is an int

			if requestedPermission == Permission.CREATE:

				match ty:
					case ResourceTypes.AE:
						# originator may be None or empty or C or S. 
						# That is okay if type is AE and this is a create request
						# Originator == None or len == 0
						if not originator or self.isAllowedOriginator(originator, CSE.registration.allowedAEOriginators):
							L.isDebug and L.logDebug('Originator for AE CREATE. OK.')
							return True
						# fall-through
					
					case ResourceTypes.CSR | ResourceTypes.CSEBaseAnnc:
						if self.isAllowedOriginator(originator, CSE.registration.allowedCSROriginators):
							L.isDebug and L.logDebug('Originator for CSR/CSEBaseAnnc CREATE. OK.')
							return True
						else:
							L.isWarn and L.logWarn(f'Originator for CSR/CSEBaseAnnc registration not found. Add "{getIdFromOriginator(originator)}" to the configuration [cse.registration].allowedCSROriginators in the CSE\'s ini file to grant access for this originator.')
							return False
				# fall-through

			if ty.isAnnounced():
				if self.isAllowedOriginator(originator, CSE.registration.allowedCSROriginators) or (parentResource and originator[1:] == parentResource.ri):
					L.isDebug and L.logDebug('Originator for Announcement. OK.')
					return True
				else:
					L.isWarn and L.logWarn('Originator for Announcement not found.')
					return False
		
		# # Check for resource == None
		# if not resource:
		# 	raise INTERNAL_SERVER_ERROR(L.logErr('Resource must not be None'))

		# Allow originator for announced resource
		if resource.isAnnounced():
			if self.isAllowedOriginator(originator, CSE.registration.allowedCSROriginators) and resource.lnk.startswith(f'{originator}/'):
				L.isDebug and L.logDebug('Announcement originator. OK.')
				return True
		
		# Allow originator if resource is announced to the originator and the request is UPDATE
		if (at := resource.at) is not None and requestedPermission == Permission.UPDATE:
			ot = f'{originator}/'
			if any(each.startswith(ot) for each in at):
				L.isDebug and L.logDebug('Announcement target originator. OK.')
				return True

		L.isDebug and L.logDebug(f'Permission check originator: {originator} ri: {resource.ri} permission: {requestedPermission} resource type: {resource.ty} type: {ty}')

		match resource.ty:

			# Allow some Originators to RETRIEVE the CSEBase
			case ResourceTypes.CSEBase if requestedPermission & Permission.RETRIEVE:
				# Allow remote CSE to RETRIEVE the CSEBase
				if originator == CSE.remote.registrarCSI:
					L.isDebug and L.logDebug(f'Grant registrar CSE Originnator {originator} to RETRIEVE CSEBase. OK.')
					return True
				if self.isAllowedOriginator(originator, CSE.registration.allowedCSROriginators):
					L.isDebug and L.logDebug(f'Grant remote CSE Orignator {originator} to RETRIEVE CSEBase. OK.')
					return True

				# Allow registered AEs to RETRIEVE the CSEBase
				# This comes last, since it is the most expensive check
				try:
					# TODO perhaps have a DB with all originators and their kind?

					# TODO add a "raw" attribute that returns the JSON, but doesn't intantiate the object
					if CSE.storage.retrieveResource(aei = originator):
						L.isDebug and L.logDebug(f'Grant registered AE Orignator {originator} to RETRIEVE CSEBase. OK.')
						return True
				except NOT_FOUND:
					pass # NOT Found is expected
			
				# Fall-through to further checks

				# TODO can we return here already?
				# TODO add a test for accessing the CSEBase by an AE + one that fails
				

			# Checking for PollingChannel
			case ResourceTypes.PCH:
				if originator != resource.getParentOriginator():
					L.isWarn and L.logWarn('Access to <PCH> resource is only granted to the parent originator.')
					return False
				return True
			
			# target is a group resource
			case ResourceTypes.GRP:
				# Check membersAccessControlPolicyIDs if provided, otherwise accessControlPolicyIDs are to be used
				if not (macp := resource.macp):
					L.isDebug and L.logDebug("MembersAccessControlPolicyIDs not provided for GRP, using AccessControlPolicyIDs")
					# fall-through to the permission checks below
				else:
					# handle the permission already checks here
					for acpRi in macp:
						if _checkACPI(originator, acpRi, requestedPermission, ty):
							return True
					L.isDebug and L.logDebug('Permission NOT granted')
					return False
				
			# target is an ACP or ACPAnnc resource
			case ResourceTypes.ACP | ResourceTypes.ACPAnnc:
				if self.checkACPSelfPermission(cast(ACP, resource), originator, requestedPermission):
					L.isDebug and L.logDebug('Self-Permission granted')
					return True

				L.isDebug and L.logDebug('Self-Permission NOT granted')
				return False

			# If subscription, check whether originator has retrieve permissions on the subscribed-to resource (parent)	
			case ResourceTypes.SUB if parentResource:
				# check whether an originator has also RETRIEVES permissions on the parent resource		
				if self.hasAccess(originator, parentResource, Permission.RETRIEVE) == False:
					return False
				# fall-through to the permission checks below
			
			case _:
				pass	# fall-through to the permission checks below

		#
		# Further permission checks
		#
		
		# When no acpi is configured for the resource
		if not (acpi := resource.acpi):
			L.isDebug and L.logDebug('Handle with missing acpi in resource')

			# if the resource *may* have an acpi but doesn't have one set
			if resource._attributes and 'acpi' in resource._attributes:

				# Check custodian attribute
				if custodian := resource.cstn:
					if custodian == originator:	# resource.custodian == originator -> all access
						L.isDebug and L.logDebug(f'Grant access for custodian: {custodian}')
						return True
					# When custodian is set, but doesn't match the originator then fall-through to fail
					L.isDebug and L.logDebug(f'Resource creator: {custodian} != originator: {originator}')
					# Fall-through to fail
					
				# Check resource creator
				else:
					if (creator := resource.getOriginator()) == originator:
						L.isDebug and L.logDebug('Grant access for creator')
						return True
					# if originator is not the original resource creator
					L.isDebug and L.logDebug(f'Resource creator: {creator} != originator: {originator}')
				# Fall-through to fail

			# resource doesn't support acpi attribute
			else:
				if resource.inheritACP:
					L.isDebug and L.logDebug('Checking parent\'s permission')
					try:
						if not parentResource:
							parentResource = CSE.dispatcher.retrieveResource(resource.pi)
						return self.hasAccess(originator, parentResource, requestedPermission, ty)
					except ResponseException as e:
						L.isWarn and L.logWarn(f'Parent resource not found: {resource.pi}: {e.dbg}')
						return False
				# Fall-through to fail

			L.isDebug and L.logDebug('Permission NOT granted for resource w/o acpi')
			return False

		#
		# Finally check the acpi
		#
		for acpRi in acpi:
			if _checkACPI(originator, acpRi, requestedPermission, ty):
				return True
			
		# no fitting permission identified
		L.isDebug and L.logDebug(f'Permission NOT granted. Originator: {originator} may not be listed in any of the linked ACPs')
		return False


	def checkAcpiUpdatePermission(self, request:CSERequest, targetResource:Resource, originator:str) -> bool:
		"""	Check whether this is actually a correct update of the acpi attribute, and whether this is actually allowed.

			Args:
				request: The original request.
				targetResource: The request target.
				originator: The request originator.
			
			Return:
				Boolean value. *True* indicates that this is an ACPI update. *False* indicates that this NOT an ACPI update. if no access is provided then an exception is raised.
			
			Raises
				`BAD_REQUEST`: If the *acpi* attribute is not the only attribute in an UPDATE request.
				`ORIGINATOR_HAS_NO_PRIVILEGE`: If the originator has no access.
		"""
		updatedAttributes = findXPath(request.pc, '{*}')	# Get the attributes under the resource element

		# Check that acpi, if present, is the only attribute
		if 'acpi' in updatedAttributes:
			if len(updatedAttributes) > 1:
				raise BAD_REQUEST(L.logDebug('"acpi" must be the only attribute in an update'))
			
			# Check whether the originator has UPDATE privileges for the acpi attribute (pvs!)
			_originator = getIdFromOriginator(originator)
			if not targetResource.acpi:
				if _originator != targetResource.getOriginator():
					raise ORIGINATOR_HAS_NO_PRIVILEGE(L.logDebug(f'No access to update acpi for originator: {originator}'))
				else:
					pass	# allowed for creating originator
			else:
				# test the current acpi whether the originator is allowed to update the acpi
				for acpRi in targetResource.acpi:
					try:
						if not (acp := CSE.dispatcher.retrieveResource(acpRi)):
							L.isWarn and L.logWarn(f'Access Check for acpi: referenced <ACP> resource not found: {acpRi}')
							continue
						if self.checkACPSelfPermission(cast(ACP, acp), _originator, Permission.UPDATE):
							break	# granted
					except ResponseException as e:
						L.isWarn and L.logWarn(f'Access Check for acpi: referenced <ACP> resource not found: {acpRi}: {e.dbg}')
						continue
				else:
					raise ORIGINATOR_HAS_NO_PRIVILEGE(L.logDebug(f'Originator: {originator} has no permission to update acpi for: {targetResource.ri}'))

			return True # True indicates that this is an ACPI update with the correct permissions
		return False	# False indicates that this NOT an ACPI update



	def checkSingleACPPermission(self, acp:ACP, originator:str, requestedPermission:Permission, ty:ResourceTypes) -> bool:
		"""	Check whether an *originator* has the requested permissions.

			Args:
				acp: The ACP resource to check.
				originator: The originator to test the permissions for.
				requestedPermission: The permissions to test.
				ty: If the resource type is given then it is checked for CREATE (as an allowed child resource type), otherwise as an allowed resource type.
			Return:
				If any of the configured *accessControlRules* of the ACP resource matches, then the originatorhas access, and *True* is returned, or *False* otherwise.
		"""
		for acr in acp['pv/acr']:

			# Check Permission-to-check first
			if requestedPermission & acr['acop'] == Permission.NONE:	# permission not fitting at all
				continue

			# Check accessControlObjectDetails
			if acod := acr.get('acod'):
				for eachAcod in acod:
					# Check type of chty
					if requestedPermission == Permission.CREATE:
						if ty is None or ty not in eachAcod.get('chty'):	# ty is an int, chty a list of ints
							continue										# for CREATE: type not in chty
					else:
						if ty is not None and ty != eachAcod.get('ty'):		# ty is an int
							continue								# any other Permission type: ty not in chty
					break # found one, so apply the next checks further down
				else:
					continue	# NOT found, so continue the next acr

				# TODO support acod/specialization

			# Check originator
			if self._checkAcor(acp, acr['acor'], originator):
				return True

		return False


	def checkACPSelfPermission(self, acp:ACP|ACPAnnc, originator:str, requestedPermission:Permission) -> bool:
		"""	Check whether an *originator* has the requested permissions to the `ACP` resource itself.

			Args:
				originator: The originator to test the permissions for.
				requestedPermission: The permissions to test.
			Return:
				If any of the configured *accessControlRules* of the ACP resource matches, then the originatorhas access, and *True* is returned, or *False* otherwise.
		"""
		# NOTE The same function also exists in ACPAnnc.py

		match acp.ty:
			case ResourceTypes.ACP:
				for permission in acp['pvs/acr']:
					if requestedPermission & permission['acop'] == 0:	# permission not fitting at all
						continue

					# Check originator
					if self._checkAcor(acp, permission['acor'], originator):
						return True
				return False

			case ResourceTypes.ACPAnnc:
				# Check for self permissions in the ACPAnnc must be done a bit differently because we 
				# don't have the optimizations that we have in the ACP resource
				for permission in acp['pvs/acr']:
					if requestedPermission & permission['acop'] == 0:	# permission not fitting at all
						continue

					# TODO check acod in pvs
					if 'all' in permission['acor'] or originator in permission['acor']:
						return True
					
					if any([ simpleMatch(originator, a) for a in permission['acor'] ]):	# check whether there is a wildcard match
						return True
				return False
		return False


	def _checkAcor(self, acp:ACP, acor:list[str], originator:str) -> bool:
		""" Check whether an originator is in the list of acor entries.
		
			Args:
				acor: The list of acor entries.
				originator: The originator to check.
				
			Return:
				True if the originator is in the list of acor entries, False otherwise.
		"""

		# Check originator
		if 'all' in acor or originator in acor:
			return True
		
		# Iterrate over all acor entries for either a group check or a wildcard check
		for a in acor:

			# Check for group. If the originator is a member of a group, then the originator has access
			if acp.getTypeForRI(a) == ResourceTypes.GRP:
				try:
					if originator in CSE.dispatcher.retrieveResource(a).mid:
						L.isDebug and L.logDebug(f'Originator found in group member')
						return True
				except ResponseException as e:
					L.logErr(f'GRP resource not found for ACP check: {a}', exc = e)
					continue # Not much that we can do here

			# Otherwise Check for wildcard match
			if simpleMatch(originator, a):
				return True
		
		# No match found
		return False


	def isAllowedOriginator(self, originator:str, allowedOriginators:List[str]) -> bool:
		""" Check whether an Originator is in the provided list of allowed originators. This list may contain regex.
			
			The hosting CSE has always access.

			Args:
				originator: The request originator.
				allowedOriginators: A list of allowed originators, which may include regex.
			
			Return:
				Boolean value indicating the result.
		"""
		if not originator or not allowedOriginators:
			return False

		_originator = getIdFromOriginator(originator)
		L.isDebug and L.logDebug(f'Originator: {_originator} - allowed originators: {allowedOriginators}')
		
		# Always allow for the hosting CSE
		if originator in [CSE.cseCsi, CSE.cseSPRelative] :
			return True

		for ao in allowedOriginators:
			if simpleMatch(_originator, ao):
				return True
		return False


	def hasAccessToPollingChannel(self, originator:str, resource:PCH|PCH_PCU) -> bool:
		"""	Check whether the originator has access to the PCU resource.
			This should be done to check the parent PCH, but the originator
			would be the same as the PCU, so we can optimize this a bit.

			Args:
				originator: The request originator
				resource: Either a PCH or PCU resource

			Return:
				Boolean indicating the result.
		"""
		return originator == resource.getOriginator()



	##########################################################################
	#
	#	Certificate handling
	#

	def _getContext(self, useTLS:bool, verifyCertificate:bool, tlsVersion:str, caCertificateFile:str, caPrivateKeyFile:str) -> ssl.SSLContext:
		"""	Depending on the configuration whether to use TLS, this method creates a new *SSLContext*
			from the configured certificates and returns it. If TLS is disabled then *None* is returned.

			Return:
				SSL / TLD context.
		"""
		context = None
		if useTLS:
			L.isDebug and L.logDebug(f'Certfile: {caCertificateFile}, KeyFile:{caPrivateKeyFile}, TLS version: {tlsVersion}')
			context = ssl.SSLContext(
							{ 	'tls1.1' : ssl.PROTOCOL_TLSv1_1,
								'tls1.2' : ssl.PROTOCOL_TLSv1_2,
								'auto'   : ssl.PROTOCOL_TLS,			# since Python 3.6. Automatically choose the highest protocol version between client & server
							}[tlsVersion.lower()]
						)
			context.load_cert_chain(caCertificateFile, caPrivateKeyFile)
			context.verify_mode = ssl.CERT_REQUIRED if verifyCertificate else ssl.CERT_NONE
		return context
	

	def getSSLContextHttp(self) -> ssl.SSLContext:
		"""	Depending on the configuration whether to use TLS, this method creates a new *SSLContext*
			from the configured certificates and returns it. If TLS is disabled then *None* is returned.

			This method is used for HTTP connections.

			Return:
				SSL / TLD context.
		"""
		L.isDebug and L.logDebug(f'Setup HTTPS SSL context.')
		return self._getContext(self.useTLSHttp, 
						  		self.verifyCertificateHttp, 
								self.tlsVersionHttp, 
								self.caCertificateFileHttp, 
								self.caPrivateKeyFileHttp)	# type: ignore


	def getSSLContextWs(self) -> ssl.SSLContext:
		"""	Depending on the configuration whether to use TLS, this method creates a new *SSLContext*
			from the configured certificates and returns it. If TLS is disabled then *None* is returned.

			This method is used for WebSocket connections.

			Return:
				SSL / TLD context.
		"""
		L.isDebug and L.logDebug(f'Setup WSS SSL context.')
		return self._getContext(self.useTLSWs,
						  		self.verifyCertificateWs,
								self.tlsVersionWs,
								self.caCertificateFileWs,
								self.caPrivateKeyFileWs)	# type: ignore


	##########################################################################
	#
	#	User authentication
	#

	def validateHttpBasicAuth(self, username:str, password:str) -> bool:
		"""	Validate the provided username and password against the configured basic authentication file.

			Args:
				username: The username to validate.
				password: The password to validate.

			Return:
				Boolean indicating the result.
		"""
		return self.httpBasicAuthData.get(username) == password


	def validateHttpTokenAuth(self, token:str) -> bool:
		"""	Validate the provided token against the configured token authentication file.

			Args:
				token: The token to validate.

			Return:
				Boolean indicating the result.
		"""
		return token in self.httpTokenAuthData


	def _readHttpBasicAuthFile(self) -> None:
		"""	Read the HTTP basic authentication file and store the data in a dictionary.
			The authentication information is stored as username:password.

			The data is stored in the `httpBasicAuthData` dictionary.
		"""
		self.httpBasicAuthData = {}
		# We need to access the configuration directly, since the http server is not yet initialized
		if Configuration.get('http.security.enableBasicAuth') and self.httpBasicAuthFile:
			try:
				with open(self.httpBasicAuthFile, 'r') as f:
					for line in f:
						if line.startswith('#'):
							continue
						if len(line.strip()) == 0:
							continue
						(username, password) = line.strip().split(':')
						self.httpBasicAuthData[username] = password.strip()
			except Exception as e:
				L.logErr(f'Error reading basic authentication file: {e}')


	def _readHttpTokenAuthFile(self) -> None:
		"""	Read the HTTP token authentication file and store the data in a dictionary.
			The authentication information is stored as a single token per line.

			The data is stored in the `httpTokenAuthData` list.
		"""
		self.httpTokenAuthData = []
		# We need to access the configuration directly, since the http server is not yet initialized
		if Configuration.get('http.security.enableTokenAuth') and self.httpTokenAuthFile:
			try:
				with open(self.httpTokenAuthFile, 'r') as f:
					for line in f:
						if line.startswith('#'):
							continue
						if len(line.strip()) == 0:
							continue
						self.httpTokenAuthData.append(line.strip())
			except Exception as e:
				L.logErr(f'Error reading token authentication file: {e}')



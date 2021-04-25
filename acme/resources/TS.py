#
#	TS.py
#
#	(c) 2021 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	ResourceType: TimeSeries
#

from __future__ import annotations
import random, string
from copy import deepcopy
from Constants import Constants as C
from Configuration import Configuration
from Types import ResourceTypes as T, Result, NotificationContentType, NotificationEventType
import Utils, CSE
from Validator import constructPolicy, addPolicy
from .Resource import *
import resources.Factory as Factory
from .AnnounceableResource import AnnounceableResource
from Types import ResponseCode as RC, JSON
from Logging import Logging

# Attribute policies for this resource are constructed during startup of the CSE
attributePolicies = constructPolicy([
	'rn', 'ty', 'ri', 'pi', 'et', 'lbl', 'ct', 'lt', 'cr', 'hld', 'acpi', 'daci', 
	'at', 'aa', 'loc'
])

tsPolicies = constructPolicy([
	'mni', 'mbs', 'mia', 'cni', 'cbs', 'pei', 'mdd', 'mdn', 'mdlt', 'mdc', 'mdt', 'cnf',
	'or'
])
attributePolicies =  addPolicy(attributePolicies, tsPolicies)


# TODO periodicIntervalDelta missing in TS-0004? Shortname for validation
# TODO periodicIntervalDelta default

# TODO Implement SUB missing data


class TS(AnnounceableResource):

	def __init__(self, dct:JSON=None, pi:str=None, create:bool=False) -> None:
		super().__init__(T.TS, dct, pi, create=create, attributePolicies=attributePolicies)

		self.resourceAttributePolicies = tsPolicies	# only the resource type's own policies

		if self.dict is not None:
			self.setAttribute('cni', 0, overwrite=False)
			self.setAttribute('cbs', 0, overwrite=False)
			if Configuration.get('cse.ts.enableLimits'):	# Only when limits are enabled
				self.setAttribute('mni', Configuration.get('cse.ts.mni'), overwrite=False)
				self.setAttribute('mbs', Configuration.get('cse.ts.mbs'), overwrite=False)
				self.setAttribute('mdn', Configuration.get('cse.ts.mdn'), overwrite=False)

		# TODO: periodicIntervalDelta : If the attribute is omitted the hosting CSE can use
		#  a local policy to determine a default value.
		self.__validating = False	# semaphore for validating


	# Enable check for allowed sub-resources
	def canHaveChild(self, resource: Resource) -> bool:
		return super()._canHaveChild(resource, 
									[ T.TSI,
									  T.SUB,
									  # <latest>
									  # <oldest>
									])


	def activate(self, parentResource:Resource, originator:str) -> Result:
		if not (res := super().activate(parentResource, originator)).status:
			return res

		# register latest and oldest virtual resources
		Logging.logDebug(f'Registering latest and oldest virtual resources for: {self.ri}')

		# add latest
		resource = Factory.resourceFromDict({}, pi=self.ri, ty=T.TS_LA).resource	# rn is assigned by resource itself
		if (res := CSE.dispatcher.createResource(resource)).resource is None:
			return Result(status=False, rsc=res.rsc, dbg=res.dbg)

		# add oldest
		resource = Factory.resourceFromDict({}, pi=self.ri, ty=T.TS_OL).resource	# rn is assigned by resource itself
		if (res := CSE.dispatcher.createResource(resource)).resource is None:
			return Result(status=False, rsc=res.rsc, dbg=res.dbg)
		
		self._validateDataDetect()
		return Result(status=True)


	def deactivate(self, originator:str) -> None:
		super().deactivate(originator)
		CSE.timeSeries.stopMonitoringTimeSeries(self)


	def update(self, dct:JSON=None, originator:str=None) -> Result:
		if not (res := super().update(dct, originator)).status:
			return res
		self._validateDataDetect(dct)
		return res

 
	def validate(self, originator:str=None, create:bool=False, dct:JSON=None) -> Result:
		if (res := super().validate(originator, create, dct)).status == False:
			return res
		Logging.logDebug(f'Validating timeSeries: {self.ri}')
		self._validateChildren()
		return Result(status=True)


	def childWillBeAdded(self, childResource:Resource, originator:str) -> Result:
		if not (res := super().childWillBeAdded(childResource, originator)).status:
			return res
		
		# Check whether the child's rn is "ol" or "la".
		if (rn := childResource['rn']) is not None and rn in ['ol', 'la']:
			return Result(status=False, rsc=RC.operationNotAllowed, dbg='resource types "latest" or "oldest" cannot be added')
	
		# Check whether the size of the TSI doesn't exceed the mbs
		if childResource.ty == T.TSI and self.mbs is not None:
			if childResource.cs is not None and childResource.cs > self.mbs:
				return Result(status=False, rsc=RC.notAcceptable, dbg='child content sizes would exceed mbs')

		# Check whether another TSI has the same dgt value set
		tsis = CSE.storage.searchByDict({ 	'ty'	: T.TSI,
											'pi'	: self.ri,
											'dgt'	: childResource.dgt
		})
		if len(tsis) > 0:
			return Result(status=False, rsc=RC.conflict, dbg=f'timeSeriesInstance with the same dgt: {childResource.dgt} already exists')

		return Result(status=True)


	# Handle the addition of new TSI. Basically, get rid of old ones.
	def childAdded(self, childResource:Resource, originator:str) -> None:
		Logging.logDebug(f'Child resource added: {childResource.ri}')
		super().childAdded(childResource, originator)
		if childResource.ty == T.TSI:	# Validate if child is TSI

			# Check for mia handling. This sets the et attribute in the TSI
			if self.mia is not None:
				# Take either mia or the maxExpirationDelta, whatever is smaller
				maxEt = Utils.getResourceDate(self.mia if self.mia <= (med := Configuration.get('cse.maxExpirationDelta')) else med)
				# Only replace the childresource's et if it is greater than the calculated maxEt
				if childResource.et > maxEt:
					childResource.setAttribute('et', maxEt)
					childResource.dbUpdate()

			self.validate(originator)
		
		# Add to monitoring if this is enabled for this TS (mdd & pei & mdt are not None, and mdd==True)
		if (mdd := self.mdd) is not None and mdd == True and self.pei is not None and self.mdt is not None:
			CSE.timeSeries.updateTimeSeries(self)


	# Handle the removal of a CIN. 
	def childRemoved(self, childResource:Resource, originator:str) -> None:
		Logging.logDebug(f'Child resource removed: {childResource.ri}')
		super().childRemoved(childResource, originator)
		if childResource.ty == T.TSI:	# Validate if child was TSI
			self._validateChildren()


	def _validateChildren(self) -> None:
		""" Internal validation and checks. This called more often then just from
			the validate() method.
		"""
		# Check whether we already are in validation the children (ie prevent unfortunate recursion by the Dispatcher)
		if self.__validating:
			return
		self.__validating = True

		cs = self.timeSeriesInstances()	# retrieve TIS child resources
		cni = len(cs)			
			
		# Check number of instances
		if (mni := self.mni) is not None:
			i = 0
			l = cni
			while cni > mni and i < l:
				Logging.logDebug(f'cni > mni: Removing <tsi>: {cs[i].ri}')
				# remove oldest
				CSE.dispatcher.deleteResource(cs[i], parentResource=self)
				cni -= 1	# decrement cni when deleting a <cin>
				i += 1
			cs = self.timeSeriesInstances()	# retrieve TSI child resources again
			cni = len(cs)

		# Calculate cbs
		cbs = 0
		for c in cs:
			cbs += c.cs

		# check size
		if (mbs := self.mbs) is not None:
			i = 0
			l = len(cs)
			while cbs > mbs and i < l:
				Logging.logDebug(f'cbs > mbs: Removing <tsi>: {cs[i].ri}')
				# remove oldest
				cbs -= cs[i]['cs']
				CSE.dispatcher.deleteResource(cs[i], parentResource=self)
				cni -= 1	# decrement cni when deleting a <tsi>
				i += 1

		# Some attributes may have been updated, so store the resource 
		self['cni'] = cni
		self['cbs'] = cbs
		self.dbUpdate()
	
		# End validating
		self.__validating = False


	def _validateDataDetect(self, dct:JSON=None) -> None:
		Logging.log('Validating data detection')

		# Check whether missing data detection is turned on 
		if (mdd := self.mdd) is not None and mdd == True:
			# When missingDataMaxNr is set
			if (mdn := self.mdn) is not None:
				self.setAttribute('mdlt', [], overwrite=False)	# add missing data list
				self.setAttribute('mdc', 0, overwrite=False)	# add missing data count
				# Monitoring is not started here, but happens when the first TSI is added
			else:
				# Remove the list and count when missing data number is not set
				self.delAttribute('mdlt')	# remove list
				self.delAttribute('mdc')	# remove counter
				# Stop monitoring happens below

		# If any of mdd, pei or mdt becomes None, or is mdd==False, then stop monitoring this TS
		if (mdd := self.mdd) is None or mdd == False or self.pei is None or self.mdt is None:
			if CSE.timeSeries.isMonitored(self.ri):
				CSE.timeSeries.stopMonitoringTimeSeries(self)
		
		# Check if mdn was changed and shorten mdlt accordingly, if exists
		if self.mdlt is not None and (newMdn := Utils.findXPath(dct, 'm2m:ts/mdn')) is not None:	# Returns None if dct is None or not found in dct
			mdlt = self.mdlt
			if (l := len(mdlt)) > newMdn:
				mdlt = mdlt[l-newMdn:]
				self['mdlt'] = mdlt
				self['mdc'] = newMdn
		
		# Check if mdt was changed in an update
		if dct is not None and 'mdt' in dct['m2m:ts']:	# mdt is in the update
			mdt = Utils.findXPath(dct, 'm2m:ts/mdt')
			isMonitored = CSE.timeSeries.isMonitored(self.ri)
			if mdt is None and isMonitored:				# it is in the update, but set to None, meaning remove the mdt from the TS
				CSE.timeSeries.stopMonitoringTimeSeries(self)
			elif mdt is not None and isMonitored:		# it is in the update and has a value, so update the monitor
				CSE.timeSeries.updateTimeSeries(self)	# This will implicitly start monitoring


		# Save changes
		self.dbUpdate()





	def timeSeriesInstances(self) -> list[Resource]:
		"""	Get all timeSeriesInstances of a timeSeries and return a sorted (by ct) list
		""" 
		return sorted(CSE.dispatcher.directChildResources(self.ri, T.TSI), key=lambda x: x.ct) # type:ignore[no-any-return]

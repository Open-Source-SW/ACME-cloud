 #
#	SemanticManager.py
#
#	(c) 2022 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	This module implements semantic service functions
#

"""	This module implements semantic service and helper functions. """

from __future__ import annotations

from typing import Sequence
from abc import ABC, abstractmethod
import base64, binascii

from ..resources.SMD import SMD
from ..services import CSE
from ..services.Logging import Logging as L
from ..etc.Types import Result, SemanticFormat


class SemanticHandler(ABC):
	"""	Abstract base class for semantic graph store handlers.
	"""

	@abstractmethod
	def validateDescription(self, description:str, format:SemanticFormat) -> Result:
		"""	Validate a semantic description.
		
			Args:
				description: A string with the semantic description.
				format: The format of the string in *description*. It must be supported.
			Return:
				A `Result` object indicating a valid description, or with an error status.
		"""
		...


	@abstractmethod
	def addDescription(self, description:str, format:SemanticFormat, id:str) -> Result:
		"""	Add a semantic description to the graph store.
		
			Args:
				description: A string with the semantic description.
				format: The format of the string in *description*. It must be supported.
				id: Identifier for the graph. It should be a resouce identifier.
			Return:
				A `Result` object. The query result is returned in its *data* attribute.
		"""
		...


	@abstractmethod
	def query(self, query:str, ids:Sequence[str], format:SemanticFormat) -> Result:
		"""	Run a SPARQL query against a graph.

			Args:
				query: SPARQL query.
				ids: List of resource / graph identifiers used to build the graph for the query.
				format: Desired serialization format for the result. It must be supported.
			Return:
				`Result` object. The serialized query result is stored in *data*.
		"""
		...


	@abstractmethod
	def reset(self) -> None:
		"""	Reset the handler, remove all graphs etc.
		"""	
		...


class SemanticManager(object):
	"""	This Class implements semantic service and helper functions. 

		Attributes:
			semanticHandler: The semantic graph store handler to be used for the CSE.
			defaultFormat: Serialization format to use as a default
	"""

	# TODO: configurable store
	# TODO: reset of DB during startup
	# TODO: shutdown
	def __init__(self) -> None:
		"""	Initialization of the SemanticManager module.
		"""
		self.semanticHandler = RdfLibHandler()
		self.defaultFormat = SemanticFormat.FF_RdfXml	# TODO configurable

		# Add a handler when the CSE is reset
		CSE.event.addHandler(CSE.event.cseReset, self.restart)	# type: ignore
		L.isInfo and L.log('SemanticManager initialized')


	def shutdown(self) -> bool:
		"""	Shutdown the Semantic Manager.
		
			Returns:
				Boolean that indicates the success of the operation
		"""
		L.isInfo and L.log('SemanticManager shut down')
		return True


	def restart(self) -> None:
		"""	Restart the Semantic Manager.
		"""
		self.semanticHandler.reset()
		L.isDebug and L.logDebug('SemanticManager restarted')



	#########################################################################
	#
	#	SMD support functions
	#

	def validateDescriptor(self, smd:SMD) -> Result:
		"""	Check that the *descriptor* attribute conforms to the syntax defined by
			the *descriptorRepresentation* attribute. 

			Todo:
				Not fully implemented yet.

			Args:
				smd: `SMD` object to use in the validation.
			Return:
				`Result` object indicating success or error.
		"""
		L.isDebug and L.logDebug('Validating descriptor')
		# Test base64 encoding is actually done during validation.
		# But since we need to decode it here anyway error handling is done
		# here as well.

		# Validate descriptorRepresentation
		# In TS-0004 this comes after the descriptor validation, but should come before it
		if smd.dcrp == SemanticFormat.IRI:
			return Result.errorResult(dbg = L.logDebug('dcrp format must not be IRI'))
		try:
			# Also store the decoded B64 string in the resource
			smd.setAttribute(smd._decodedDsp, _desc := base64.b64decode(smd.dsp, validate = True).decode('UTF-8').strip())
		except binascii.Error as e:
			return Result.errorResult(dbg = L.logDebug(f'Invalid base64-encoded descriptor: {str(e)}'))
		return self.semanticHandler.validateDescription(_desc, smd.dcrp)

	
	def validateSPARQL(self, query:str) -> Result:
		"""	Validate wether an input string is a valid SPARQL query.

			Todo:
				Not implemented yet.

			Args:
				query: String with the SPARQL query to validate.
			Return:
				`Result` object indicating success or error. In case of an error the *rsc* 
				is set to *INVALID_SPARQL_QUERY*.
		"""
		L.isDebug and L.logDebug(f'Validating SPARQL request')
		L.isWarn and L.logWarn('Validation of SMD.semanticOpExec is not implemented')
		return Result.successResult()


	def validateValidationEnable(self, smd:SMD) -> Result:
		"""	Check and handle the setting of the *validationEnable* attribute.

			Todo:
				Not fully implemented yet.

			Args:
				smd: `SMD` object to use in the validation. **Attn**: This procedure might update and change the provided *smd* object.
			Return:
				`Result` object indicating success or error.
		"""
		# The default for ACME is to not enable validation
		if smd.vlde is None:
			smd.setAttribute('vlde', False)
			smd.setAttribute('svd', False)
		return Result.successResult()


	def addDescriptor(self, smd:SMD) -> Result:
		"""	Perform the semantic validation of the <`SMD`> resource

			Todo:
				Not fully implemented yet.

			Args:
				smd: `SMD` resource object to use in the validation. **Attn**: This procedure might update and change the provided *smd* object.
			Return:
				Result object indicating success or error.
		"""
		L.isDebug and L.logDebug('Adding descriptor')

		res = self.semanticHandler.addDescription(smd.attribute(smd._decodedDsp), smd.dcrp, smd.ri)
		if not res.status and smd.vlde:
			return res
		
		# TODO more validation!
		# b) If the validationEnable attribute is set as true, the hosting CSE shall perform the semantic validation process in
		# 	the following steps according to clause 7.10.2 in oneM2M TS-0034 [50]. Otherwise, skip the following steps.
		# c) Check if the addressed <semanticDescriptor> resource is linked to other <semanticDescriptor> resources on a remote CSE
		#	by the relatedSemantics attribute or by triples with annotation property m2m:resourceDescriptorLink in descriptor attribute.
		#	This process shall consider the recursive links.
		#	- If yes, the Hosting CSE shall generate an Update request primitive with itself as the Originator and with the 
		# 		Content parameter set to the addressed <semanticDescriptor> resource representation, and send it to the <semanticValidation>
		# 		virtual resource URI on the CSE which hosts the referenced ontology (following the ontologyRef attribute) of the addressed
		# 		<semanticDescriptor> resource (see details in clause 7.4.48.2.3). After receiving the response primitive, i.e. 
		# 		the validation result, go to step k. If no response primitive was received due to time-out or other exceptional cases,
		# 		the hosting CSE shall generate a Response Status Code indicating a "TARGET_NOT_REACHABLE" error.
		#	-  If no, perform the following steps.
		# d) Access the semantic triples from the descriptor attribute of the received <semanticDescriptor> resource.
		# e) Access the ontology referenced in the ontologyRef attribute of the received <semanticDescriptor> resource.
		# 	- If the ontology referenced by the ontologyRef attribute is an external ontology, not locally hosted by the Hosting CSE,
		# 		the Hosting CSE shall retrieve it using the corresponding protocol and identifier information specified in the ontologyRef attribute.
		#	- If the referenced ontology cannot be retrieved within a reasonable time (as defined by a local policy), the Hosting CSE shall generate
		# 		 a Response Status Code indicating an "ONTOLOGY_NOT_AVAILABLE" error.
		# f) Retrieve any local linked <semanticDescriptor> resources of the received <semanticDescriptor> resource following the URI(s) in
		#  the relatedSemantics attribute (if it exists) and the URI(s) in the triples with annotation property m2m:resourceDescriptorLink (if there are any).
		#	- Repeat this step recursively to Retrieve any further local linked <semanticDescriptor> resources.
		# 	- If the local linked <semanticDescriptor> resources cannot be retrieved within a reasonable time (which is subject to a local policy),
		# 		the Hosting CSE shall generate a Response Status Code indicating a "LINKED_SEMANTICS_NOT_AVAILABLE" error.
		# g) Retrieve the semantic triples from the descriptor attribute of the local linked <semanticDescriptor> resource.
		# h) Retrieve the referenced ontologies of the local linked <semanticDescriptor> resources following the URI(s) in ontologyRef attribute of
		# 	the linked <semanticDescriptor> resources; If the referenced ontologies cannot be retrieved within a reasonable time (as defined by 
		# 	a local policy), the Hosting CSE shall generate a Response Status Code indicating an "ONTOLOGY_NOT_AVAILABLE" error.
		# i) Combine all the semantic triples of the addressed and local linked <semanticDescriptor> resources as the set of semantic triples to be
		# 	validated, and combine all the referenced ontologies as the set of ontologies to validate the semantic triples against.
		# j) Check all the aspects of semantic validation according to clause 7.10.3 in oneM2M TS-0034 [50] based upon the semantic triples and 
		# 	referenced ontology. If any problem occurs, the Hosting CSE shall generate a Response Status Code indicating an "INVALID_SEMANTICS" error.

		return Result.successResult()
	

	#########################################################################
	#
	#	SMD discovery functions
	#

	# def getAggregatedDescriptions(self, smds:Sequence[SMD]) -> Sequence[str]:
	# 	# TODO doc
	# 	return [ base64.decodebytes(bytes(smd.dsp, 'utf-8')).decode('utf-8') 
	# 			 for smd in smds ]
	

	def executeSPARQLQuery(self, query:str, smds:Sequence[SMD], format:SemanticFormat = None) -> Result:
		"""	Run a SPARQL query against a list of <`SMD`> resources.
		
			Args:
				query: String with the SPARQL query.
				smds: A list of <`SMD`> resources which are to be aggregated for the query.
				format: Serialization format to use.
			Return:
				`Result` object. If successful, the *data* attribute contains the serialized result of the query.
		"""
		L.isDebug and L.logDebug('Performing SPARQL query')
		return self.semanticHandler.query(query, 
										  [ smd.ri for smd in smds ], 
										  self.defaultFormat if not format else format)
		# aggregatedGraph = self.semanticHandler.getAggregatedGraph([ smd.ri for smd in smds ])
		# qres = self.semanticHandler.query(query, aggregatedGraph).data
		# return Result(status = True, data = qres.serialize(format='xml').decode('UTF-8'))


###############################################################################

import rdflib
from rdflib.plugins.stores.memory import Memory
from rdflib.term import URIRef


class RdfLibHandler(SemanticHandler):
	"""	A SemanticHandler implementation for the *rdflib* library.

		Attributes:
			store: The store that stores the graphs.
			graph: The root graph for the CSE.
	"""

	supportedFormats =	{ SemanticFormat.FF_RdfXml : 'xml',
						}
	"""	A map between the *SemanticFormat* enum and the rdflib string representation. Only the
		supported formats are listed here.
	"""

	storeIdentifier =	'acme'
	"""	The identifier for the graph stores."""


	def __init__(self) -> None:
		"""	Initializer for the RdfLibHandler class.
		"""
		super().__init__()
		L.isInfo and L.log('Using RDFLIB handler for semantics')

		self.store = Memory() 								# type:ignore [no-untyped-call]
		self._openStore()

		self.graph = rdflib.Dataset(store = self.store)		# type:ignore [no-untyped-call]
		# TODO memory or db...
	

	#
	#	Implementation of the abstract methods
	#

	def validateDescription(self, description:str, format:SemanticFormat) -> Result:
		if not (_format := self.getFormat(format)):
			return Result.errorResult(dbg = L.logWarn(f'Unsupported format: {format} for semantic descriptor'))

		# Parse once to validate, but throw away the result
		try:
			rdflib.Graph().parse(data = description, format = _format)
		except Exception as e:
			return Result.errorResult(dbg = L.logWarn(f'Invalid descriptor: {str(e)}'))
		return Result.successResult()
	

	def addDescription(self, description:str, format:SemanticFormat, id:str) -> Result:
		if not (_format := self.getFormat(format)):
			return Result.errorResult(dbg = L.logWarn(f'Unsupported format: {format} for semantic descriptor'))
		
		# Parse into its own graph
		try:
			g = rdflib.Graph(store = self.store, identifier = id)
			g.parse(data = description, format = _format)
		except Exception as e:
			L.logErr('', exc = e)
			return Result.errorResult(dbg = L.logWarn(f'Invalid descriptor: {str(e)}'))
		return Result.successResult()


	def query(self, query:str, ids:Sequence[str], format:SemanticFormat) -> Result:
		L.isDebug and L.logDebug(f'Querying graphs')
		if not (_format := self.getFormat(format)):
			return Result.errorResult(dbg = L.logWarn(f'Unsupported format: {format} for result'))

		# Aggregate a new graph for the query
		aggregatedGraph = self.getAggregatedGraph(ids)
		L.logWarn(f'{len(aggregatedGraph)}')

		# Query the graph
		qres = aggregatedGraph.query(query)

		# Serialize the result in the desired format and return
		return Result(status = True, data = qres.serialize(format = _format).decode('UTF-8'))


	def reset(self) -> None:
		L.isDebug and L.logDebug(f'Removing all graphs from the store')
		self.store.close()								# type:ignore [no-untyped-call]
		self.store.destroy(self.storeIdentifier)		# type:ignore [no-untyped-call]
		self._openStore()

	#
	#	Handler-internal methods
	#

	def getFormat(self, format:SemanticFormat) -> str|None:
		"""	Return a representation of a semantic format supported by the graph framework.

			Args:
				format:	The semantic format.
			Return:
				A string representation of the *format* that is supported, or *None* if unsupported.
		"""
		return self.supportedFormats.get(format)


	def getGraph(self, id:str) -> rdflib.Graph|None:
		"""	Find and return the stored graph with the given identifier.

			Args:
				id: The graph's identifier.
			Return:
				A *Graph* object, or None.
		"""
		return self.graph.get_graph(URIRef(id))


	def getAggregatedGraph(self, ids:Sequence[str]) -> rdflib.Dataset|None:
		"""	Return an aggregated graph with all the triple for the individuel
			graphs for the list of resources indicated by their resource IDs. 

			Args:
				ids: List of <semanticDescriptor> resource Identifiers.
			Return:
				Return a *DataSet* object with the aggregated graph, or None.

		"""
		L.isDebug and L.logDebug(f'Aggregating graphs for ids: {ids}')
		# create a common store for the aggregation
		dataset = rdflib.Dataset(store = Memory())		# type:ignore [no-untyped-call]
		for id in ids:
			if not (g := self.getGraph(id)):
				L.logErr(f'Graph for id: {id} not found')
				return None
			[ dataset.add(_g) for _g in g ]
				
		#L.logDebug(dataset.serialize(format='xml'))
		return dataset		

	#
	#	Graph store methods
	#

	def _openStore(self) -> None:
		"""	Open the graph store.
		"""
		self.store.open(self.storeIdentifier, create = True)

#
#	ACMEContainerTree.py
#
#	(c) 2023 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
"""	This module defines the *Resources* view for the ACME text UI.
"""
from __future__ import annotations
from typing import List, Tuple, Optional, Any, cast

import json

from textual import events
from textual.app import ComposeResult
from textual.widgets import Tree as TextualTree, Static, TabbedContent, TabPane, Label, Button
from textual.widgets.tree import TreeNode
from textual.containers import Container, Vertical, Horizontal
from textual.screen import ModalScreen
from textual.binding import Binding
from rich.syntax import Syntax
from ..runtime import CSE
from ..resources.Resource import Resource
from ..textui.ACMEContainerRequests import ACMEViewRequests
from ..etc.ResponseStatusCodes import ResponseException
from ..etc.Types import ResourceTypes
from ..etc.Constants import RuntimeConstants as RC
from ..helpers.TextTools import commentJson, limitLines
from .ACMEContainerCreate import ACMEContainerCreate
from .ACMEContainerDelete import ACMEContainerDelete
from .ACMEContainerUpdate import ACMEContainerUpdate
from .ACMEContainerDiagram import ACMEContainerDiagram
from .ACMEContainerResourceServices import ACMEContainerResourceServices



class ACMEResourceTree(TextualTree):


	def __init__(self, *args:Any, **kwargs:Any) -> None:

		self.parentContainer = kwargs.pop('parentContainer', None)
		super().__init__(*args, **kwargs)

	
	# def on_mount(self) -> None:
	# 	self.root.expand()

	def on_show(self) -> None:
		from ..textui.ACMETuiApp import ACMETuiApp
		self._app = cast(ACMETuiApp, self.app)


	def _update_tree(self) -> None:
		if not self.visible:
			return
		self.clear()
		self.auto_expand = False
		self.select_node(None)
		prevType = ''
		for resource in self._retrieve_resource_children(RC.cseRi):
			ty = resource[0].ty
			if ty != prevType and not ResourceTypes.isVirtualResource(ty):
				self.root.add(f'[{self._app.objectColor} b]{ResourceTypes.fullname(ty)}[/]', allow_expand = False)
				prevType = ty
			self.root.add(resource[0].rn, data = resource[0].ri, allow_expand = resource[1])
		self._update_content(self.cursor_node.data)


	def on_tree_node_highlighted(self, node:TextualTree.NodeHighlighted) -> None:
		try:
			if node.node.data:
				self._update_content(node.node.data)
			else:
				# No data means this is a type section
				self._update_type_section(str(node.node.label))
		except ResponseException as e:
			# self.parentContainer.resourceView.update(f'ERROR: {e.dbg}')
			self.parentContainer.updateResourceView(error = f'ERROR: {e.dbg}')



	def on_tree_node_expanded(self, node:TextualTree.NodeSelected) -> None:
		self._buildNodeChildren(node.node)
		# node.node._children = []	# no available method?
		# prevType = ''
		# for resource in self._retrieve_resource_children(node.node.data):
		# 	ty = resource[0].ty
		# 	if ty != prevType and not ResourceTypes.isVirtualResource(ty):
		# 		node.node.add(f'[{self._app.objectColor} b]{ResourceTypes.fullname(ty)}[/]', allow_expand = False)
		# 		prevType = ty
		# 	node.node.add(resource[0].rn, data = resource[0].ri, allow_expand = resource[1])
	

	def on_tree_node_hover(self, event:events.MouseMove) -> None:
		self.parentContainer.setResourceHeader('Resources')


	_virtualResourcesParameter = {
		ResourceTypes.CNT_LA: (ResourceTypes.CIN, False),
		ResourceTypes.CNT_OL: (ResourceTypes.CIN, True),
		ResourceTypes.FCNT_LA: (ResourceTypes.FCI, False),
		ResourceTypes.FCNT_OL: (ResourceTypes.FCI, True),
		ResourceTypes.TS_OL: (ResourceTypes.TSI, True),
		ResourceTypes.TS_LA: (ResourceTypes.TSI, False),
	}


	def refreshNode(self, node:TreeNode) -> None:
		if not self.visible:
			return
		self._buildNodeChildren(node)


	def refreshCurrentNode(self) -> None:
		self.refreshNode(self.cursor_node)


	def refreshCurrentParrentNode(self) -> None:
		parentNode = self.cursor_node.parent
		if parentNode:
			self.refreshNode(parentNode)


	def _update_content(self, ri:str) -> None:
		try:
			resource = CSE.dispatcher.retrieveLocalResource(ri)

			# retrieve the latest/oldest instance of some virtual resources
			if (_params := self._virtualResourcesParameter.get(resource.ty)):
				if (_r := CSE.dispatcher.retrieveLatestOldestInstance(resource.pi, _params[0], oldest = _params[1])):
					resource = _r
				else:
					resource = None
		except ResponseException as e:
			self._update_tree()
			return
		
		# Update the resource view and other views
		self.parentContainer.updateResource(resource)

		# Update the header
		self.parentContainer.setResourceHeader(f'{resource.rn} ({_getResourceTypeAsString(resource)})' if resource else '')
		self.parentContainer.setResourceSubtitle(f'{resource.getSrn()} ({resource.ri})' if resource else '')

		# Set the visibility of the tabs
		try:
			self.parentContainer.tabs.show_tab('tree-tab-requests')
		except:
			pass


	def _update_type_section(self, label:str) -> None:
		"""	Update the resource view with a type section.
		
			Args:
				label: The label of the type section.
		"""
		self.parentContainer.setResourceHeader(f'{label} Resources')
		# self.parentContainer.resourceView.update('')
		self.parentContainer.updateResourceView()
		self.parentContainer.tabs.hide_tab('tree-tab-diagram')
		self.parentContainer.tabs.hide_tab('tree-tab-requests')
		self.parentContainer.tabs.hide_tab('tree-tab-services')	
		self.parentContainer.tabs.hide_tab('tree-tab-delete')
		self.parentContainer.tabs.hide_tab('tree-tab-update')
		self.parentContainer.tabs.hide_tab('tree-tab-create')


	def _buildNodeChildren(self, node:TreeNode) -> None:
		try:
			node.remove_children()
		except KeyError:
			pass # Catch key error that might occur hear. Not much that we can do here
		#self._app.notify(str([ x.id for x in node.children]))
		# node._children = []	# no available method?
		prevType = ''
		for resource in self._retrieve_resource_children(node.data):
			ty = resource[0].ty
			if ty != prevType and not ResourceTypes.isVirtualResource(ty):
				node.add(f'[{self._app.objectColor} b]{ResourceTypes.fullname(ty)}[/]', allow_expand = False)
				prevType = ty
			node.add(resource[0].rn, data = resource[0].ri, allow_expand = resource[1])
	


	def _retrieve_resource_children(self, ri:str) -> List[Tuple[Resource, bool]]:
		"""	Retrieve the children of a resource and return a sorted list of tuples.
		
			Each tuple contains a resource and a boolean indicating if the resource
			has children itself.

			Sort order is: virtual and instance resources first, then by type and name.
			
			Args:
				ri: The resource id of the parent resource.
				
			Returns:
				A sorted list of tuples (resource, hasChildren).
		"""
		result:List[Tuple[Resource, bool]] = []
		chs = [ x for x in CSE.dispatcher.retrieveDirectChildResources(ri) if not x.ty in [ ResourceTypes.GRP_FOPT, ResourceTypes.PCH_PCU ]]
		
		# Sort resources: virtual and instance resources first, then by type and name
		top = []
		rest = []
		for resource in chs:
			if ResourceTypes.isVirtualResource(resource.ty) or ResourceTypes.isInstanceResource(resource.ty):
				top.append(resource)
			else:
				rest.append(resource)
		rest.sort(key = lambda r: (r.ty, r.rn))
		chs = top + rest

		for resource in chs:
			result.append((resource, len([ x for x in CSE.dispatcher.retrieveDirectChildResources(resource.ri)  ]) > 0))
		return result


class ACMEContainerTree(Container):

	BINDINGS = 	[ Binding('r', 'refresh_resources', 'Refresh'),
				#   Binding('o', 'overlay', 'Overlay'),
		  
				
				# TODO copy resource
				# TODO delete

				# delete requests
				]


	from ..textui import ACMETuiApp

	def __init__(self, id:str) -> None:
		"""	Initialize the view.
		
			Args:
				id:	The view ID.
		"""
		super().__init__(id = id)

		
		self.currentResource:Resource = None

		# Create some views and widgets beforehand
		self._treeView = ACMEResourceTree(RC.cseRn, data = RC.cseRi, id = 'tree-view', parentContainer = self)
		self._treeTabs = TabbedContent(id = 'tree-tabs')

		self._treeTabRequests = TabPane('Requests', id = 'tree-tab-requests')
		self._treeTabServices = TabPane('Services', id = 'tree-tab-services')
		self._treeTabCreate = TabPane('CREATE', id = 'tree-tab-create')
		self._treeTabUpdate = TabPane('UPDATE', id = 'tree-tab-update')
		self._treeTabDelete = TabPane('DELETE', id = 'tree-tab-delete')
		self._treeTabDiagram = TabPane('Diagram', id = 'tree-tab-diagram')
		self._treeTabDiagramView = ACMEContainerDiagram(refreshCallback = lambda: self.updateResource(self.currentResource), 
														id = 'tree-tab-diagram-view')

		self._treeTabResourceServices = ACMEContainerResourceServices(id = 'tree-tab-resource-services')
		self._treeTabResourceCreate = ACMEContainerCreate(id = 'tree-tab-resource-create')
		self._treeTabResourceUpdate = ACMEContainerUpdate(id = 'tree-tab-resource-update')
		self._treeTabResourceDelete = ACMEContainerDelete(id = 'tree-tab-resource-delete')
		self._treeTabRequestsView = ACMEViewRequests(id = 'tree-tab-requests-view')
		self._resourceViewContainer = Container(id = 'resource-view-container')
		self._resourceView = Static(id = 'resource-view', expand = True)



	def compose(self) -> ComposeResult:
		with Container():
			yield self._treeView
			with self._treeTabs:
				with TabPane('Resource', id = 'tree-tab-resource'):
					with self._resourceViewContainer:
						yield self._resourceView

				with self._treeTabRequests:
					yield self._treeTabRequestsView

				with self._treeTabServices:
					yield self._treeTabResourceServices

				with self._treeTabCreate:
					yield self._treeTabResourceCreate

				with self._treeTabUpdate:
					yield self._treeTabResourceUpdate

				with self._treeTabDelete:
					yield self._treeTabResourceDelete
				
				with self._treeTabDiagram:
					yield self._treeTabDiagramView

				
	@property
	def resourceContainer(self) -> Container:
		return self._resourceViewContainer


	def on_show(self) -> None:
		from ..textui.ACMETuiApp import ACMETuiApp
		self._app = cast(ACMETuiApp, self.app)
		"""	The application. """

		self.resourceTree.root.expand()
		self.update()
		self.resourceTree.focus()


	def on_click(self, event:events.Click) -> None:
		"""Handle Click events.

			Args:
				event: The Click event.
		"""

		# When clicking on the container of the resource view
		if self.screen.get_widget_at(event.screen_x, event.screen_y)[0] is (_cnt := self.resourceContainer):
			
			# When clicking on the bottom border: Copy the structured or unstructured resource identifier
			if event.y == _cnt.outer_size.height-1:
				v = self.currentResource.getSrn()
				ri = self.currentResource.ri
				t = 'Structured Resource Identifier Copied'
				if event.x > len(v) + 3 and event.x < len(v) + 6 + len(ri):
					v = ri
					t = 'Resource Identifier Copied'
				self._app.copyToClipboard(v)
				self._app.showNotification(v, t, 'information')

			# When clicking on the top border: Copy the resource name or type
			elif event.y == 0:
				v = self.currentResource.rn
				rt = ResourceTypes.fullname(self.currentResource.ty)
				rt = _getResourceTypeAsString(self.currentResource)
				t = 'Resource Name Copied'
				if event.x > len(v) + 3 and event.x < len(v) + 6 + len(rt):
					v = rt
					t = 'Resource Type Copied'
				self._app.copyToClipboard(v)
				self._app.showNotification(v, t, 'information')

		# When clicking on the resource view
		elif self.screen.get_widget_at(event.screen_x, event.screen_y)[0] is self.resourceView:
			self._app.copyToClipboard(v := json.dumps(self.currentResource.asDict(sort = True), indent = 2))
			self._app.showNotification(limitLines(v, 5), 'Resource Copied', 'information')


	def action_refresh_resources(self) -> None:
		self._app.showNotification('Refreshing resources', 'info', 'information', 2)
		self.update()

			
	def update(self) -> None:
		self.resourceTree._update_tree()
	

	def refreshCurrentNode(self) -> None:
		self.resourceTree.refreshCurrentNode()

	
	def refreshCurrentParrentNode(self) -> None:
		self.resourceTree.refreshCurrentParrentNode()


	def updateResource(self, resource:Optional[Resource] = None) -> None:
		# Store the resource for later


		if resource:
			self.currentResource = resource


		# Add attribute explanations
		if resource:
			# Update the requests view
			self._update_requests(self.currentResource.ri)

			# Update DELETE view
			self.deleteView.updateResource(self.currentResource)
			self.deleteView.disabled = False

			# Update the UPDATE view
			self.updateView.updateResource(self.currentResource)
			self.updateView.disabled = False

			# Update the CREATE view
			self.createView.updateResource(self.currentResource)
			self.createView.disabled = False

			# Update the services view
			self.servicesView.updateResource(self.currentResource)

			# Update Diagram view
			try:

				# Show some default tabs
				self.tabs.show_tab('tree-tab-services')

				match self.currentResource.ty:
					case ResourceTypes.CSEBase:
						# Don't allow to send request to the CSE resource - hide all tabs
						self.tabs.hide_tab('tree-tab-update')
						self.tabs.hide_tab('tree-tab-delete')
						self.tabs.hide_tab('tree-tab-diagram') 

					case ResourceTypes.CNT | ResourceTypes.TS:
						instances = CSE.dispatcher.retrieveDirectChildResources(self.currentResource.ri, [ResourceTypes.CIN, ResourceTypes.TSI])
						
						# The following lines may fail if the content cannot be converted to a float or a boolean.
						# This is expected! This just means that any content is not a number and we cannot raw a diagram.
						# The exception is caught below and the diagram view is hidden.
						try:
							values = [float(r.con) for r in instances]
						except ValueError:
							# Number (int or float) failed. Now try boolean
							values = []
							for r in instances:
								_con = r.con
								if isinstance(_con, str):
									if _con.lower() in ['true', 'on', 'yes', 'high']:
										values.append(1)
									elif _con.lower() in ['false', 'off', 'no', 'low']:
										values.append(0)
									else:
										self.app.bell()
										raise ValueError	# not a "boolean" value
								else:
									raise ValueError	# Not a string in the first place

						dates = [r.ct for r in instances]

						self.diagram.setData(values, dates)
						self.diagram.plotGraph()
						self.tabs.show_tab('tree-tab-diagram')
						self.tabs.show_tab('tree-tab-create')
						self.tabs.show_tab('tree-tab-delete')
						self.tabs.show_tab('tree-tab-update')

					case ResourceTypes.CIN | ResourceTypes.TSI | ResourceTypes.FCI:
						self.tabs.hide_tab('tree-tab-diagram') 
						self.tabs.show_tab('tree-tab-create')
						self.tabs.hide_tab('tree-tab-update')
						self.tabs.show_tab('tree-tab-delete')
					
					case ResourceTypes.CNT_LA | ResourceTypes.CNT_OL | ResourceTypes.FCNT_LA | ResourceTypes.FCNT_OL | ResourceTypes.TS_OL | ResourceTypes.TS_LA:
						self.tabs.hide_tab('tree-tab-diagram') 
						self.tabs.show_tab('tree-tab-create')
						self.tabs.hide_tab('tree-tab-update')
						self.tabs.show_tab('tree-tab-delete')

					case _:
						self.tabs.hide_tab('tree-tab-diagram') 
						self.tabs.show_tab('tree-tab-create')
						self.tabs.show_tab('tree-tab-update')
						self.tabs.show_tab('tree-tab-delete')
			except:
				try:
					self.tabs.hide_tab('tree-tab-diagram')
					self.tabs.show_tab('tree-tab-update')
					self.tabs.show_tab('tree-tab-delete')
					self.tabs.show_tab('tree-tab-create')
				except:
					pass

		else:

			# Disable the views
			self.tabs.hide_tab('tree-tab-diagram') 
			self.tabs.hide_tab('tree-tab-create')
			self.tabs.hide_tab('tree-tab-update')
			self.tabs.hide_tab('tree-tab-delete')
			self.tabs.hide_tab('tree-tab-services')

			# Update the requests view with an empty string
			self._update_requests('')
			
		# Add syntax highlighting and add to the view
		# self.resourceView.update(Syntax(jsns, 'json', theme = self.app.syntaxTheme))	# type: ignore [attr-defined]
		self.updateResourceView(commentJson(self.currentResource.asDict(sort = True), 
								explanations = self.app.attributeExplanations,	# type: ignore [attr-defined]
								getAttributeValueName = lambda a, v: CSE.validator.getAttributeValueName(a, v, self.currentResource.ty if self.currentResource else None)))	# type: ignore [attr-defined]


	def updateResourceView(self, value:Optional[str|Resource] = None, error:Optional[str] = None) -> None:
		if value:
			if isinstance(value, Resource):
				value = commentJson(value.asDict(sort = True), 
									explanations = self.app.attributeExplanations,	# type: ignore [attr-defined]
									getAttributeValueName = lambda a, v: CSE.validator.getAttributeValueName(a, v, value.ty if value else None))	# type: ignore [attr-defined]
			self.resourceView.update(Syntax(value, 'json', theme = self.app.syntaxTheme))	# type: ignore [attr-defined]
		elif error:
			self.resourceView.update(error)
		else:
			self.resourceView.update('')

	
	async def on_tabbed_content_tab_activated(self, event:TabbedContent.TabActivated) -> None:
	#async def on_tabs_tab_activated(self, event:Tabs.TabActivated) -> None:
		"""Handle TabActivated message sent by Tabs."""
		# self.app.debugConsole.update(event.tab.id)

		match self.tabs.active:
			case 'tree-tab-requests':
				self._update_requests()
				self.requestView.updateBindings()
			case 'tree-tab-resource':
				pass
			case 'tree-tab-diagram':
				pass
			case 'tree-tab-delete':
				pass
			case 'tree-tab-services':
				pass

		self.app.updateFooter()	# type:ignore[attr-defined]


	def _update_requests(self, ri:Optional[str] = None) -> None:
		if self.tabs.active == 'tree-tab-requests':
			self.requestView.currentRI = ri if ri else self.resourceTree.cursor_node.data
			self.requestView.updateRequests()
			self.requestView.requestList.focus()
			# select the first request
			if len(self.requestView.requestList) > 0:	
				self.requestView.requestList.index = 0
	

	def setResourceHeader(self, header:str) -> None:
		"""	Set the header of the resource view.
		
			Args:
				header: The header to set.
		"""
		self.resourceContainer.border_title = header

	
	def setResourceSubtitle(self, subtitle:str) -> None:
		"""	Set the subtitle of the resource view.
		
			Args:
				subtitle: The subtitle to set.
		"""
		self.resourceContainer.border_subtitle = subtitle
	

	@property
	def tabs(self) -> TabbedContent:
		return self._treeTabs


	@property
	def resourceTree(self) -> ACMEResourceTree:
		return self._treeView


	@property
	def createView(self) -> ACMEContainerCreate:
		return self._treeTabResourceCreate


	@property
	def deleteView(self) -> ACMEContainerDelete:
		return self._treeTabResourceDelete


	@property
	def updateView(self) -> ACMEContainerUpdate:
		return self._treeTabResourceUpdate

	@property
	def servicesView(self) -> ACMEContainerResourceServices:
		return self._treeTabResourceServices


	@property
	def requestView(self) -> ACMEViewRequests:
		return self._treeTabRequestsView


	@property
	def resourceView(self) -> Static:
		return self._resourceView


	@property
	def diagram(self) -> ACMEContainerDiagram:
		return self._treeTabDiagramView

	
# TODO move the following to a more generic dialog module
class ACMEDialog(ModalScreen):
	BINDINGS = [('escape', 'pop_dialog', 'Close')]

	def __init__(self, message:str = 'Really?', buttonOK:str = 'OK', buttonCancel:str = 'Cancel') -> None:
		super().__init__()
		self.message = message
		self.buttonOK = buttonOK
		self.buttonCancel = buttonCancel
		self.width = len(self.message) + 10 if len(self.message) + 10 > 50 else 50


	def compose(self) -> ComposeResult:
		with (_v := Vertical(id = 'confirm')):
			yield Label(self.message, id = 'confirm-label')
			with Horizontal(id = 'confirm-buttons'):
				yield Button(self.buttonOK, variant = 'success', id = 'confirm-ok')
				yield (_b := Button(self.buttonCancel, variant = 'primary', id = 'confirm-cancel'))
		_b.focus()
		_v.styles.width = self.width


	def action_pop_dialog(self) -> None:
		self.dismiss(False)


	def on_button_pressed(self, event: Button.Pressed) -> None:
		if event.button.id == 'confirm-ok':
			self.dismiss(True)
		else:
			self.dismiss(False)

#
# Helper functions
#

def _getResourceTypeAsString(resource:Resource) -> str:
	"""	Return the resource type as a string.
		If the resource is a flex container, the specialization is added.

		Args:
			resource: The resource to get the type for.

		Returns:
			The resource type as a string.
	"""
	if resource.ty == ResourceTypes.FCNT:
		# Put the specialization in the header if it is a flex container
		return f'{ResourceTypes.fullname(resource.ty)} - {resource.typeShortname.split(":")[0]}:{CSE.validator.getFlexContainerSpecialization(resource.typeShortname)[1]}'
	else:
		return ResourceTypes.fullname(resource.ty)


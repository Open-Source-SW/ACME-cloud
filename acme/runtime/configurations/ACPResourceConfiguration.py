#
#	ACPResourceConfiguration.py
#
#	(c) 2024 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	ACP Resource configurations
#

from __future__ import annotations
from typing import Optional

import configparser

from ...runtime.Configuration import Configuration
from ...runtime.configurations.ModuleConfiguration import ModuleConfiguration
from ...etc.Types import Permission


class ACPResourceConfiguration(ModuleConfiguration):

	def readConfiguration(self, parser:configparser.ConfigParser, config:Configuration) -> None:

		#	Defaults for Access Control Policies

		config.resource_acp_selfPermission = parser.getint('resource.acp', 'selfPermission', fallback = Permission.DISCOVERY+Permission.NOTIFY+Permission.CREATE+Permission.RETRIEVE)


	def validateConfiguration(self, config:Configuration, initial:Optional[bool] = False) -> None:
		pass


#!/usr/bin/env python
"""Modified version for the job-sender package to avoid
problems when dealing with clusters mounted over old 
distributions. 

Original package from Martin Blech:
https://github.com/martinblech/xmltodict
"""
# Used when 'from xmltodict import *'
__all__ = [ "xmltodict"]
# Used when 'import xmltodict' (really want this?)
import xmltodict as xmltodict_jb

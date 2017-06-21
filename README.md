job-sender 
==========
Configuring and managing ATLAS, CMS, ILC and other particle physics
software related cluster jobs. 
This package has been separated from their original repository at 
[PyAnUtils](https://github.com/duartej/PyAnUtils) given the
                                                                  
author: Jordi Duarte-Campderros (Nov.2014)
email : jorge.duarte.campderros -at- cern

INSTALLATION
------------
The package provides a (Distutils) 'setup.py' to build and install it. Just 
```bash
  % python setup.py install [--user] 
```
The --user option is used when you don't have root privilegies (or you 
don't want to install the package in the global site-packages directories). 
The package will be installed inside of the user directory '$HOME/.local'. 
You have to modify the enviroment variables: 
```bash
  % export PYTHONPATH=$PYTHONPATH:$HOME/.local/lib
  % export PATH=$PATH:$HOME/.local/bin
```
in order to use the new scripts and modules.

USAGE
-----
TO BE FILLED

CONTENT
-------
TO BE FILLED

Use *help* function for detailed information of each module and script.
 

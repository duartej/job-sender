from distutils.core import setup, Extension


setup(name='job-sender',
        version='1.0',
        description='Job cluster utilities',
        author='Jordi Duarte-Campderros',
        author_email='Jordi.Duarte.Campderros@cern.ch',
        url='https://github.com/duartej/job-sender',
        # See https://docs.python.org/2/distutils/setupscript.html#listing-whole-packages
        # for changes in the package distribution
        package_dir={'job_sender':'python', 'xmltodict_jb': 'xmltodict_jb'},
        packages = ['job_sender','xmltodict_jb' ],
        scripts=['bin/clustermanager'],
        )

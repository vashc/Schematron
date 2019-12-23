from distutils.core import setup

setup(
    name='SchemaChecker',
    version='0.0.3.dev110',
    package_dir={'': 'src'},
    packages=['schemachecker',
              'schemachecker.edo',
              'schemachecker.fns',
              'schemachecker.stat',
              'schemachecker.pfr',
              'schemachecker.fss',
              'schemachecker.rar'],
    install_requires=open('requirements.txt').read().splitlines(),
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.txt').read(),
)

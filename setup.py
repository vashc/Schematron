from distutils.core import setup

setup(
    name='SchemaChecker',
    version='0.0.1b4',
    packages=['schematron', ],
    install_requires=open('requirements.txt').read().splitlines(),
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.txt').read(),
)

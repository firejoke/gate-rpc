from setuptools import setup
import gaterpc


setup(
    name='gaterpc',
    version=gaterpc.__version__,
    packages=['gaterpc'],
    url='https://github.com/firejoke/gate-rpc',
    license='BSD-3-Clause',
    author='Shi Fan',
    author_email='firejokeshi@gmail.com',
    description='A RPC software based on ZeroMQ with built-in Majordomo.',
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
    ]
)

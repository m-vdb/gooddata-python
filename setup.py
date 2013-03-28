import os
from setuptools import setup, find_packages


try:
    f = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
    long_description = f.read().strip()
    f.close()
except IOError:
    long_description = None

setup(
    name='gooddata-python',
    version='1.0',
    url="https://github.com/Work4Labs/gooddata-python",
    description='Python client for GoodData REST API',
    long_description=long_description,
    author='Work4 Labs Analytics',
    author_email='mvergerdelbove@work4labs.com',
    license='BSD',
    keywords='gooddata python api'.split(),
    platforms='any',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    test_suite='tests.runtests.main',
    install_requires=['requests', 'simplejson'],
)

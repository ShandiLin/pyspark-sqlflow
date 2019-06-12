from __future__ import print_function

import os

from setuptools import setup, find_packages, Command
from setuptools.command.install import install
from wheel.bdist_wheel import bdist_wheel


class Install(install):
    """
        custom install command
        usage
            python setup.py install [--pip-args="{pip-args}"]
        notice
            pip-args: use "" to wrap your pip arguments
    """
    description = "install with specific pypi server"
    user_options = install.user_options + [('pip-args=', None, 'args for pip')]

    def initialize_options(self):
        install.initialize_options(self)
        self.pip_args = None

    def finalize_options(self):
        install.finalize_options(self)
        if self.pip_args is None:
            print('pip_args not set, using default https://pypi.org/simple/')

    def run(self):
        for dep in self.distribution.install_requires:
            install_cmd = "pip install {} --disable-pip-version-check --no-cache-dir".format(dep)
            if self.pip_args is not None:
                install_cmd += ' ' + self.pip_args
            os.system(install_cmd)
        install.run(self)


class BdistWheel(bdist_wheel):
    """
        custom bdist_wheel command
        usage
            python setup.py bdist_wheel [--pip-args="{pip-args}"]
        notice
            pip-args: use "" to wrap your pip arguments
    """
    description = "build wheel with specific pypi server"
    user_options = bdist_wheel.user_options + [('pip-args=', None, 'args for pip')]

    def initialize_options(self):
        bdist_wheel.initialize_options(self)
        self.pip_args = None

    def finalize_options(self):
        bdist_wheel.finalize_options(self)
        if self.pip_args is None:
            print('pip_args not set, using default https://pypi.org/simple/')

    def run(self):
        for dep in self.distribution.install_requires:
            install_cmd = "pip install {} --disable-pip-version-check --no-cache-dir".format(dep)
            if self.pip_args is not None:
                install_cmd += ' ' + self.pip_args
            os.system(install_cmd)
        bdist_wheel.run(self)


class Clean(Command):
    """
        custom clean command
        usage:
            python setup.py clean [-e|--egg]
    """
    description = "Cleans build files"
    user_options = [('egg', 'e', "Arguments to clean with .eggs folder")]

    def initialize_options(self):
        self.egg = None

    def finalize_options(self):
        if self.egg is None:
            print('not deleting .eggs folder')

    def run(self):
        cmd = dict(
            build="find ./ -name 'build' -exec rm -rf {} +",
            egg_info="find . -name '*.egg-info' -not -path './venv/*' -exec rm -rf {} +",
            dist="find ./ -name 'dist' -exec rm -rf {} +"
        )
        if self.egg is not None:
            cmd['eggs'] = "find ./ -name '.eggs' -exec rm -rf {} +"

        for key in cmd:
            print('remove {} folder ...'.format(key))
            os.system(cmd[key])


setup(
    name="pyspark-sqlflow",
    version="1.0",
    author="ShauHsuan Lin",
    description="write sql config and process with pyspark, connect through JDBC",
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7"
    ],
    keywords=["etl", "python", "sql", "spark", "pyspark"],
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=2.7",
    install_requires=['click',
                      'stringcase',
                      'toml',
                      'dateutils',
                      'jinja2'],
    scripts=['pyspark_sqlflow/bin/sqlflow'],
    zip_safe=False,
    cmdclass={'install': Install,
              'bdist_wheel': BdistWheel,
              'clean': Clean}
)

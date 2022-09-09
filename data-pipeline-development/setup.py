from setuptools import setup, find_packages

setup(
    name='datapipeline',
    version='0.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    url='',
    license='',
    author='ranjitkanwar,
    author_email='ranjit.kanwar@gmail.com',
    description='Data Pipeline', install_requires=['awswrangler']
)

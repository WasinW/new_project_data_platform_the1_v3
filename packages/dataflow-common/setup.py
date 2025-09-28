# # setup.py
# """Setup configuration for dataflow_common package"""

# # from setuptools import setup, find_packages
# import setuptools

# import os

# # Read version from __version__.py
# version = {}
# with open(os.path.join('dataflow_common', '__version__.py')) as fp:
#     exec(fp.read(), version)

# # Read long description from README
# with open('README.md', 'r', encoding='utf-8') as fh:
#     long_description = fh.read()

# setup(
#     name='dataflow_common',
#     version='1.0.0',
#     # version=version['__version__'],
#     author='THE1 Data Engineering',
#     author_email='data-engineering@the1.co.th',
#     description='Common modules for THE1 Dataflow pipelines',
#     packages=setuptools.find_packages(),  # จะหา dataflow_common โดยอัตโนมัติ
#     long_description=long_description,
#     long_description_content_type='text/markdown',
#     url='https://github.com/the1/dataflow-common',
#     classifiers=[
#         'Development Status :: 4 - Beta',
#         'Intended Audience :: Developers',
#         'License :: OSI Approved :: Apache Software License',
#         'Programming Language :: Python :: 3',
#         'Programming Language :: Python :: 3.8',
#         'Programming Language :: Python :: 3.9',
#         'Programming Language :: Python :: 3.10',
#     ],
#     install_requires=[
#         # 'apache-beam[gcp]>=2.50.0,<3.0.0',
#         'apache-beam[gcp]==2.59.0',
#         # 'google-cloud-bigquery>=3.0.0,<4.0.0',
#         'google-cloud-bigquery==3.25.0',
#         # 'google-cloud-bigtable>=2.0.0,<3.0.0',
#         'google-cloud-bigtable==2.23.0',
#         'google-cloud-pubsub>=2.0.0,<3.0.0',
#         'google-cloud-storage>=2.0.0,<3.0.0',
#         'pyyaml>=6.0.0,<7.0.0',
#     ],
#     python_requires='>=3.8',
#     # packages=find_packages(exclude=['tests*']),
#     # packages=['dataflow_common'],  # Include local package
#     # package_dir={'dataflow_common': 'packages/dataflow-common/dataflow_common'}
#     extras_require={
#         'dev': [
#             'pytest>=7.0.0',
#             'pytest-cov>=4.0.0',
#             'black>=23.0.0',
#             'flake8>=6.0.0',
#             'mypy>=1.0.0',
#             'build>=0.10.0',
#             'twine>=4.0.0',
#         ],
#     },
#     entry_points={
#         'console_scripts': [
#             'dataflow-common-version=dataflow_common.__version__:print_version',
#         ],
#     },
#     include_package_data=True,
#     zip_safe=False,
# )
from setuptools import setup, find_packages

setup(
    name='dataflow_common',
    version='1.0.0',
    packages=find_packages(),
    # install_requires=[
    #     'google-cloud-bigquery',  # ใส่เฉพาะ dependency ที่แพ็กเกจนี้ต้องใช้
    #     'google-cloud-bigtable',
    #     # ไม่ต้องใส่ apache-beam
    # ],
    install_requires=[
        # 'apache-beam[gcp]==2.59.0',
        # 'google-cloud-bigquery>=3.0.0,<4.0.0',
        'google-cloud-bigquery==3.25.0',
        # 'google-cloud-bigtable>=2.0.0,<3.0.0',
        'google-cloud-bigtable==2.23.0',
        'google-cloud-pubsub>=2.0.0,<3.0.0',
        'google-cloud-storage>=2.0.0,<3.0.0',
        'pyyaml>=6.0.0,<7.0.0',
    ],

)

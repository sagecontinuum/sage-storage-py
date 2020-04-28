from distutils.core import setup

setup(
    name='sage-storage-py',
    version='0.1',
    description='SAGE storage client library',
    url='https://github.com/sagecontinuum/sage-storage-py',
    install_requires=[
        'requests',
    ],
    packages=[
        'sage_storage',
    ],
    include_package_data=True,
)
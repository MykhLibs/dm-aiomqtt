from setuptools import setup, find_packages


def readme():
    with open('README.md', 'r') as f:
        return f.read()


setup(
    name='dm-aiomqtt',
    version='0.0.0',
    author='dimka4621',
    author_email='mismartconfig@gmail.com',
    description='This is my custom aiomqtt client',
    long_description=readme(),
    long_description_content_type='text/markdown',
    url='https://pypi.org/project/dm-aiomqtt',
    packages=find_packages(),
    install_requires=[
        'dm-logger==0.4.2',
        'aiomqtt==2.0.0'
    ],
    classifiers=[
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent'
    ],
    keywords='dm aiomqtt',
    project_urls={
        'GitHub': 'https://github.com/DIMKA4621/dm-aiomqtt'
    },
    python_requires='>=3.8'
)

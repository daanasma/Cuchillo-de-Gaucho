from setuptools import setup, find_packages 
 
setup( 
    name="cuchillo_de_gaucho",
    version="0.1", 
    packages=find_packages(), 
    install_requires=[],  # List any dependencies here, e.g., 'numpy' 
    author="Daan Asma",
    author_email="nope",
    description="A collection of useful utility functions.", 
    long_description=open("README.md").read(), 
    long_description_content_type="text/markdown", 
    url="https://github.com/daanasma/cuchillo-de-gaucho", 
    classifiers=[ 
        "Programming Language :: Python :: 3", 
        "License :: OSI Approved :: MIT License", 
        "Operating System :: OS Independent", 
    ], 
) 
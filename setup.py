import setuptools

def readme():
    with open('README.md') as f:
        return f.read()

setuptools.setup(
    name="Case_Dispatcher",
    version="0.1",
    author="Jon Hudlow",
    author_email="jon@lovejustice.ngo",
    description="A package for updating and prioritizing human trafficking case data.",
    long_description=readme(),
    key_words='Human trafficking cases investigations Searchlight'
    url="https://github.com/jahudlow/Data_Fellowship_Project",
    packages = find_packages(exclude=['contrib', 'docs', 'tests*']),
    entry_points={
                                            'console_scripts': [
                                                'update_cd = update_cd.__main__:main'
                                            ]
                                        },
)
    python_requires='>=3'
    classifiers=[
        "Development Status :: 3 - Alpha",
        'Intended Audience :: Love Justice - internal',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    test_suite='nose.collector',
    tests_require=['nose', 'nose-cover3']
)

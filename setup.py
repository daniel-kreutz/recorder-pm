import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="recorder-pm",                # Package name, e.g., pip install recorder-pm
    version="0.5.6",
    author="Chen Wang",
    author_email="wangvsa@gmail.com",
    description="Utilities for processing Recorder traces",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/daniel-kreutz/recorder-pm",
    packages=['recorder_pm'],                  # package for import: after installaion, import recorder_pm
    #package_data = {'recorder_pm': ['*.h']},   # *.h by default will not be copied, we use this to ship it.
    scripts=['bin/recorder-report'],
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: University of Illinois/NCSA Open Source License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7',
)


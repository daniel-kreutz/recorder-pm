recorder-viz
=============

This is a python package which contains tools for processing [Recorder](https://github.com/uiuc-hpc/Recorder) traces.

Installation and Visualization
-------------

`recorder-pm` relies on Recorder and a few python libraries to run.
Please see the document [here](https://recorder.readthedocs.io/latest/postprocessing.html#post-processing-and-visualization).

Below are some example graphs generated from the [FLASH](http://flash.uchicago.edu) traces.
![example graphs](https://raw.githubusercontent.com/wangvsa/recorder-viz/main/tests/showoff.jpg)

Usage
-------------

```shell
recorder-metrics -i=path/to/trace -o=path/to/report
```
